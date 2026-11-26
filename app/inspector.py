"""CLOSE_WAIT 连接巡检：基于 Linux /proc 的两轮采样实现。

设计要点
--------
1. 只读 /proc，不向目标进程发信号、不操作任何 FD，对业务零干扰。
2. 每轮对每个受查 PID：
   - 记录身份信息（starttime、comm），starttime 用于识别 PID 换代；
   - 读取该 PID 网络命名空间下的 TCP 表（/proc/<pid>/net/tcp，仅 IPv4）；
   - 扫描 /proc/<pid>/fd，收集 socket:[inode] -> [fd...] 归属。
3. 两轮都采到、且两轮状态均为 CLOSE_WAIT 的同一连接才输出，天然只列一次。
4. 第二轮仍存活且 starttime 未变的 PID 才算"确认仍持有"；
   中途退出或 PID 换代的 PID 进 unconfirmable，其余 PID 照常采集。
5. 所有 proc 路径都基于 ``proc_root``，测试时可指向伪造的 proc 目录树。
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# /proc/net/tcp 中 st 字段的状态码 -> 名称
TCP_STATES: Dict[str, str] = {
    "01": "ESTABLISHED",
    "02": "SYN_SENT",
    "03": "SYN_RECV",
    "04": "FIN_WAIT1",
    "05": "FIN_WAIT2",
    "06": "TIME_WAIT",
    "07": "CLOSE",
    "08": "CLOSE_WAIT",
    "09": "LAST_ACK",
    "0A": "LISTEN",
    "0B": "CLOSING",
    "0C": "NEW_SYN_RECV",
}

CLOSE_WAIT = "08"


class ProcError(Exception):
    """读取 procfs 失败（权限、进程消失等）。"""


@dataclass(frozen=True)
class TcpConnection:
    """一条 IPv4 TCP 连接（TCP 表中的一行）。"""

    slot: int  # /proc/net/tcp 行首的 sl 序号
    local_ip: str
    local_port: int
    remote_ip: str
    remote_port: int
    state: str  # 十六进制状态码，如 "08"
    inode: int

    @property
    def state_name(self) -> str:
        return TCP_STATES.get(self.state, f"UNKNOWN({self.state})")

    @property
    def key(self) -> Tuple[str, int, str, int]:
        """同一连接的身份：四元组。"""
        return (self.local_ip, self.local_port, self.remote_ip, self.remote_port)

    def to_dict(self) -> dict:
        return {
            "local": f"{self.local_ip}:{self.local_port}",
            "remote": f"{self.remote_ip}:{self.remote_port}",
            "state": self.state_name,
            "inode": self.inode,
        }


@dataclass
class ProcIdentity:
    """受查进程在某一轮采样时的身份快照。"""

    pid: int
    starttime: int  # /proc/<pid>/stat 第 22 字段（clock ticks）
    comm: str


@dataclass
class PidSnapshot:
    """单个 PID 在一轮采样中的完整结果。"""

    identity: Optional[ProcIdentity]
    connections: Dict[int, TcpConnection] = field(default_factory=dict)
    # socket inode -> 该进程持有它的 FD 列表
    socket_fds: Dict[int, List[int]] = field(default_factory=dict)
    error: Optional[str] = None  # 本轮该 PID 不可读/不存在的原因


@dataclass
class InspectionResult:
    """两轮巡检的最终结果。"""

    pids: List[int]
    interval_seconds: float
    close_wait_connections: List[dict]
    unconfirmable_pids: List[dict]
    rounds: List[dict]

    def to_dict(self) -> dict:
        return {
            "pids": self.pids,
            "interval_seconds": self.interval_seconds,
            "rounds": self.rounds,
            "close_wait_connections": self.close_wait_connections,
            "unconfirmable_pids": self.unconfirmable_pids,
        }


def _hex_to_ip(hexaddr: str) -> str:
    """把 /proc/net/tcp 的小端十六进制 IPv4 地址转成点分十进制。"""
    raw = bytes.fromhex(hexaddr)
    # 内核按主机字节序（小端）存储四个字节，反转即网络序
    return ".".join(str(b) for b in reversed(raw))


def parse_tcp_table(text: str) -> Dict[int, TcpConnection]:
    """解析 /proc/net/tcp 文本，返回 socket inode -> 连接。

    LISTEN 等没有 inode（inode=0）的行会被跳过；同一 inode 理论上唯一，
    若重复则保留后出现的行。
    """
    table: Dict[int, TcpConnection] = {}
    lines = text.splitlines()
    for line in lines[1:]:  # 跳过表头
        parts = line.split()
        if len(parts) < 10:
            continue
        try:
            slot = int(parts[0].rstrip(":"))
            local_ip_hex, local_port_hex = parts[1].split(":")
            remote_ip_hex, remote_port_hex = parts[2].split(":")
            state = parts[3].upper()
            inode = int(parts[9])
        except (ValueError, IndexError):
            continue  # 容忍格式异常的行
        if inode == 0:
            continue
        table[inode] = TcpConnection(
            slot=slot,
            local_ip=_hex_to_ip(local_ip_hex),
            local_port=int(local_port_hex, 16),
            remote_ip=_hex_to_ip(remote_ip_hex),
            remote_port=int(remote_port_hex, 16),
            state=state,
            inode=inode,
        )
    return table


def _read_proc_stat(path: str) -> Tuple[int, str]:
    """解析 /proc/<pid>/stat，返回 (starttime, comm)。"""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        text = fh.read()
    # comm 可能含空格或括号，按最后一个 ')' 切分
    rparen = text.rfind(")")
    if rparen == -1:
        raise ProcError(f"malformed stat file: {path}")
    comm = text[text.find("(") + 1 : rparen]
    fields = text[rparen + 2 :].split()
    # ')' 之后第一个字段是 state（第 3 字段），starttime 是第 22 字段，
    # 因此下标为 22 - 3 = 19
    if len(fields) < 20:
        raise ProcError(f"stat file too short: {path}")
    starttime = int(fields[19])
    return starttime, comm


def collect_socket_fds(fd_dir: str) -> Dict[int, List[int]]:
    """扫描 /proc/<pid>/fd，返回 socket inode -> [fd, ...]。

    非 socket 类型的 FD 忽略；符号链接读取失败（FD 竞争消失）容忍跳过。
    """
    result: Dict[int, List[int]] = {}
    try:
        entries = os.listdir(fd_dir)
    except (FileNotFoundError, ProcessLookupError, PermissionError) as exc:
        raise ProcError(str(exc)) from exc
    for name in entries:
        try:
            fd = int(name)
        except ValueError:
            continue
        try:
            target = os.readlink(os.path.join(fd_dir, name))
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
            continue
        if not target.startswith("socket:["):
            continue
        try:
            inode = int(target[len("socket:[") : -1])
        except ValueError:
            continue
        result.setdefault(inode, []).append(fd)
    for fds in result.values():
        fds.sort()
    return result


class ProcInspector:
    """两轮 CLOSE_WAIT 巡检器。

    Parameters
    ----------
    proc_root:
        procfs 挂载点，生产环境为 ``/proc``，测试可指向伪造目录。
    """

    def __init__(self, proc_root: str = "/proc") -> None:
        self.proc_root = proc_root

    def _pid_dir(self, pid: int) -> str:
        return os.path.join(self.proc_root, str(pid))

    def snapshot_pid(self, pid: int) -> PidSnapshot:
        """对单个 PID 采一轮：身份 + TCP 表 + socket FD 归属。"""
        pid_dir = self._pid_dir(pid)
        stat_path = os.path.join(pid_dir, "stat")
        try:
            starttime, comm = _read_proc_stat(stat_path)
        except FileNotFoundError:
            return PidSnapshot(
                identity=None, error="process not found (exited or wrong pid)"
            )
        except (PermissionError, ProcessLookupError) as exc:
            return PidSnapshot(identity=None, error=f"stat unreadable: {exc}")
        except (ValueError, OSError) as exc:
            return PidSnapshot(identity=None, error=f"stat parse failed: {exc}")

        identity = ProcIdentity(pid=pid, starttime=starttime, comm=comm)

        # 每个进程读自己命名空间下的 TCP 表；读不到则该进程本轮无连接数据
        connections: Dict[int, TcpConnection] = {}
        tcp_path = os.path.join(pid_dir, "net", "tcp")
        try:
            with open(tcp_path, "r", encoding="utf-8", errors="replace") as fh:
                connections = parse_tcp_table(fh.read())
        except FileNotFoundError:
            pass
        except (PermissionError, ProcessLookupError, OSError):
            pass  # 进程可能在读取中途退出，FD 阶段会再次暴露问题

        try:
            socket_fds = collect_socket_fds(os.path.join(pid_dir, "fd"))
        except ProcError as exc:
            return PidSnapshot(identity=identity, error=f"fd scan failed: {exc}")

        return PidSnapshot(
            identity=identity, connections=connections, socket_fds=socket_fds
        )

    def snapshot_round(self, pids: List[int]) -> Dict[int, PidSnapshot]:
        return {pid: self.snapshot_pid(pid) for pid in pids}

    def inspect(
        self, pids: List[int], interval_seconds: float
    ) -> InspectionResult:
        """执行两轮采样并汇总结果。"""
        # 去重且保持调用方给定的顺序，便于报告稳定
        unique_pids = list(dict.fromkeys(pids))
        round1 = self.snapshot_round(unique_pids)
        if interval_seconds > 0:
            time.sleep(interval_seconds)
        round2 = self.snapshot_round(unique_pids)

        rounds_summary = [
            self._round_summary(1, round1),
            self._round_summary(2, round2),
        ]

        unconfirmable: Dict[int, dict] = {}

        def mark_unconfirmable(pid: int, reason: str) -> None:
            # 同一 PID 多个原因时保留最严重/最早的一条
            unconfirmable.setdefault(
                pid,
                {
                    "pid": pid,
                    "reason": reason,
                    "round1": self._identity_of(round1.get(pid)),
                    "round2": self._identity_of(round2.get(pid)),
                },
            )

        # 1) 判定每个 PID 第二轮是否仍可确认（存活且未换代）
        confirmed_pids: List[int] = []
        for pid in unique_pids:
            s1, s2 = round1.get(pid), round2.get(pid)
            if s1 is not None and s1.error is not None and s1.identity is None:
                mark_unconfirmable(pid, f"round 1 unreadable: {s1.error}")
                continue
            if s2 is None or s2.identity is None:
                mark_unconfirmable(
                    pid,
                    "process exited between the two rounds"
                    if (s2 is None or s2.error)
                    else "process missing in round 2",
                )
                continue
            if (
                s1 is not None
                and s1.identity is not None
                and s1.identity.starttime != s2.identity.starttime
            ):
                mark_unconfirmable(
                    pid,
                    "pid reused: starttime changed between rounds (process replaced)",
                )
                continue
            confirmed_pids.append(pid)

        # 2) 找出两轮均为 CLOSE_WAIT 的同一连接（按四元组匹配）
        #    r1 连接索引：key -> (inode, conn)
        r1_index: Dict[Tuple[str, int, str, int], TcpConnection] = {}
        for snap in round1.values():
            for conn in snap.connections.values():
                if conn.state == CLOSE_WAIT:
                    r1_index[conn.key] = conn

        r2_index: Dict[Tuple[str, int, str, int], TcpConnection] = {}
        for pid in confirmed_pids:
            snap = round2[pid]
            for conn in snap.connections.values():
                if conn.state == CLOSE_WAIT:
                    r2_index[conn.key] = conn

        report: List[dict] = []
        for key in sorted(r1_index.keys() & r2_index.keys()):
            conn_r1 = r1_index[key]
            conn_r2 = r2_index[key]

            holders: List[dict] = []
            for pid in confirmed_pids:
                snap = round2[pid]
                fds = snap.socket_fds.get(conn_r2.inode)
                if not fds:
                    continue
                holders.append(
                    {
                        "pid": pid,
                        "comm": snap.identity.comm if snap.identity else None,
                        "fds": fds,
                    }
                )

            report.append(
                {
                    "connection": conn_r2.to_dict(),
                    "round1_inode": conn_r1.inode,
                    "round2_inode": conn_r2.inode,
                    "inode_stable": conn_r1.inode == conn_r2.inode,
                    "holders_round2": holders,
                }
            )

        return InspectionResult(
            pids=unique_pids,
            interval_seconds=interval_seconds,
            close_wait_connections=report,
            unconfirmable_pids=[unconfirmable[p] for p in unique_pids if p in unconfirmable],
            rounds=rounds_summary,
        )

    @staticmethod
    def _identity_of(snap: Optional[PidSnapshot]) -> Optional[dict]:
        if snap is None or snap.identity is None:
            return None
        return {
            "pid": snap.identity.pid,
            "comm": snap.identity.comm,
            "starttime": snap.identity.starttime,
            "error": snap.error,
        }

    @staticmethod
    def _round_summary(index: int, snapshots: Dict[int, PidSnapshot]) -> dict:
        ok, failed = [], []
        close_wait = 0
        for pid, snap in snapshots.items():
            if snap.identity is None:
                failed.append({"pid": pid, "error": snap.error})
            else:
                ok.append(pid)
                close_wait += sum(
                    1 for c in snap.connections.values() if c.state == CLOSE_WAIT
                )
        return {
            "round": index,
            "sampled_pids": ok,
            "failed_pids": failed,
            "close_wait_sockets_seen": close_wait,
        }

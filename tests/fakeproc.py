"""构造伪造的 /proc 目录树，用于确定性单元测试。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


def ip_to_hex(ip: str) -> str:
    """点分十进制 -> /proc/net/tcp 使用的小端十六进制。"""
    parts = [int(x) for x in ip.split(".")]
    return "".join(f"{b:02X}" for b in reversed(parts))


def tcp_row(
    slot: int,
    local: str,
    remote: str,
    state_hex: str,
    inode: int,
    uid: int = 1000,
) -> str:
    lip, lport = local.rsplit(":", 1)
    rip, rport = remote.rsplit(":", 1)
    return (
        f"   {slot}: {ip_to_hex(lip)}:{int(lport):04X} "
        f"{ip_to_hex(rip)}:{int(rport):04X} {state_hex} "
        f"00000000:00000000 00:00000000 00000000 {uid} 0 {inode} "
        f"100000000000 0 0 0 10 0"
    )


TCP_HEADER = (
    "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when "
    "retrnsmt   uid  timeout inode"
)


@dataclass
class FakeProc:
    root: str
    # pid -> {"comm":..., "starttime":..., "tcp_rows":[...], "fds": {fd: inode}}
    pids: Dict[int, dict] = field(default_factory=dict)

    def add_pid(
        self,
        pid: int,
        comm: str = "holder",
        starttime: int = 1000,
        tcp_rows: Optional[List[str]] = None,
        fds: Optional[Dict[int, int]] = None,
    ) -> None:
        self.pids[pid] = {
            "comm": comm,
            "starttime": starttime,
            "tcp_rows": tcp_rows or [],
            "fds": dict(fds or {}),
        }
        self._write_pid(pid)

    def _write_pid(self, pid: int) -> None:
        spec = self.pids[pid]
        pid_dir = os.path.join(self.root, str(pid))
        net_dir = os.path.join(pid_dir, "net")
        fd_dir = os.path.join(pid_dir, "fd")
        os.makedirs(net_dir, exist_ok=True)
        os.makedirs(fd_dir, exist_ok=True)

        # /proc/<pid>/stat（字段数需与真实文件一致，starttime 在第 22 列；
        # ')' 之后从第 3 列 state 开始，故 starttime 位于其后下标 19）
        stat = (
            f"{pid} ({spec['comm']}) S 1 {pid} {pid} 0 -1 4194304 0 0 0 "
            f"0 0 0 0 20 0 1 0 0 {spec['starttime']} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
        )
        with open(os.path.join(pid_dir, "stat"), "w", encoding="utf-8") as fh:
            fh.write(stat)

        with open(os.path.join(net_dir, "tcp"), "w", encoding="utf-8") as fh:
            fh.write(TCP_HEADER + "\n")
            fh.write("\n".join(spec["tcp_rows"]))
            if spec["tcp_rows"]:
                fh.write("\n")

        for fd, inode in spec["fds"].items():
            link = os.path.join(fd_dir, str(fd))
            if os.path.lexists(link):
                os.unlink(link)
            os.symlink(f"socket:[{inode}]", link)

    def remove_pid(self, pid: int) -> None:
        """模拟进程退出：删除整个 /proc/<pid> 目录。"""
        import shutil

        shutil.rmtree(os.path.join(self.root, str(pid)))
        self.pids.pop(pid, None)

    def replace_starttime(self, pid: int, new_starttime: int) -> None:
        """模拟 PID 换代：同 PID 写入不同 starttime。"""
        self.pids[pid]["starttime"] = new_starttime
        self._write_pid(pid)

    def rewrite_tcp(self, pid: int, tcp_rows: List[str]) -> None:
        """模拟连接状态在两轮之间变化。"""
        self.pids[pid]["tcp_rows"] = tcp_rows
        self._write_pid(pid)

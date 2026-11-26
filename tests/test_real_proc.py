"""针对真实 /proc 的冒烟测试：采测试进程自身，验证与内核格式一致。"""

from __future__ import annotations

import os

from app.inspector import ProcInspector, parse_tcp_table, TCP_STATES


def test_parse_real_proc_net_tcp():
    """真实 /proc/net/tcp 必须能被无异常解析，且状态码均可识别。"""
    with open("/proc/net/tcp", "r", encoding="utf-8") as fh:
        table = parse_tcp_table(fh.read())
    # 环境中可能没有任何带 inode 的连接，但解析不能抛异常
    for conn in table.values():
        assert conn.state in TCP_STATES
        assert "." in conn.local_ip and "." in conn.remote_ip
        assert 0 <= conn.local_port <= 65535


def test_snapshot_self_via_real_proc():
    """对当前进程做一轮快照：stat/fd 都应可读，FD 全部为非负整数。"""
    snap = ProcInspector("/proc").snapshot_pid(os.getpid())
    assert snap.error is None
    assert snap.identity is not None
    assert snap.identity.pid == os.getpid()
    assert snap.identity.starttime > 0
    # 自身至少持有 0/1/2 三个 FD；socket 类 FD 必须能解析出 inode
    for inode, fds in snap.socket_fds.items():
        assert inode > 0
        assert all(isinstance(fd, int) and fd >= 0 for fd in fds)


def test_inspect_self_two_rounds():
    """两轮采自身不应产生 unconfirmable，且结构完整。"""
    result = ProcInspector("/proc").inspect([os.getpid()], interval_seconds=0)
    assert result.unconfirmable_pids == []
    assert [r["round"] for r in result.rounds] == [1, 2]
    assert result.pids == [os.getpid()]
    for entry in result.close_wait_connections:
        assert entry["connection"]["state"] == "CLOSE_WAIT"
        assert entry["holders_round2"]  # 至少有持有者

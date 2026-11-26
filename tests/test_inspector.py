"""巡检核心逻辑的单元测试（全部基于伪造 procfs，无需 Docker/root）。"""

from __future__ import annotations

import os

import pytest

from app.inspector import ProcInspector, parse_tcp_table, _hex_to_ip
from tests.fakeproc import FakeProc, tcp_row

CLOSE_WAIT = "08"
ESTABLISHED = "01"
LISTEN = "0A"

C1 = ("10.0.0.2:40000", "10.0.0.1:9000")  # 共享 CLOSE_WAIT
C2 = ("10.0.0.2:40001", "10.0.0.1:9001")  # 仅第一轮 CLOSE_WAIT（第二轮变 ESTABLISHED 不可能，改消失）


@pytest.fixture()
def fake(tmp_path):
    return FakeProc(str(tmp_path / "proc"))


def _close_wait_row(slot, conn, inode):
    return tcp_row(slot, conn[0], conn[1], CLOSE_WAIT, inode)


def test_hex_ip_roundtrip():
    assert _hex_to_ip("0100A8C0") == "192.168.0.1"
    assert _hex_to_ip("0200000A") == "10.0.0.2"


def test_parse_tcp_table_skips_header_and_inode_zero():
    text = "\n".join(
        [
            "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
            tcp_row(0, "0.0.0.0:80", "0.0.0.0:0", LISTEN, 0),
            tcp_row(1, "10.0.0.2:40000", "10.0.0.1:9000", CLOSE_WAIT, 555),
        ]
    )
    table = parse_tcp_table(text)
    assert set(table) == {555}
    conn = table[555]
    assert conn.state_name == "CLOSE_WAIT"
    assert conn.local_ip == "10.0.0.2"
    assert conn.local_port == 40000
    assert conn.remote_ip == "10.0.0.1"
    assert conn.remote_port == 9000


def test_shared_close_wait_listed_once_with_all_holders(fake):
    """同一 inode 的 CLOSE_WAIT 被两个 PID 持有：只列一条，holder 全列出。"""
    rows = [_close_wait_row(0, C1, 3001)]
    fake.add_pid(101, comm="holder", starttime=11, tcp_rows=rows, fds={3: 3001})
    fake.add_pid(102, comm="holder", starttime=22, tcp_rows=list(rows), fds={7: 3001})

    result = ProcInspector(fake.root).inspect([101, 102], interval_seconds=0)

    assert result.unconfirmable_pids == []
    assert len(result.close_wait_connections) == 1
    entry = result.close_wait_connections[0]
    assert entry["inode_stable"] is True
    assert entry["connection"]["state"] == "CLOSE_WAIT"
    holders = {h["pid"]: h for h in entry["holders_round2"]}
    assert holders[101]["fds"] == [3]
    assert holders[102]["fds"] == [7]
    assert holders[101]["comm"] == "holder"


def test_only_one_round_close_wait_is_not_reported(fake):
    """第一轮 CLOSE_WAIT、第二轮该连接消失：不输出。"""
    fake.add_pid(
        201,
        starttime=10,
        tcp_rows=[_close_wait_row(0, C2, 4002)],
        fds={4: 4002},
    )
    inspector = ProcInspector(fake.root)

    # 手动控制两轮：先取第一轮，再改写 TCP 表
    r1 = inspector.snapshot_round([201])
    assert r1[201].connections[4002].state_name == "CLOSE_WAIT"
    fake.rewrite_tcp(201, [tcp_row(0, C2[0], C2[1], ESTABLISHED, 4002)])
    r2 = inspector.snapshot_round([201])
    assert r2[201].connections[4002].state_name == "ESTABLISHED"

    # 走完整 inspect（此时两轮都是 ESTABLISHED），应无 CLOSE_WAIT 输出
    result = inspector.inspect([201], interval_seconds=0)
    assert result.close_wait_connections == []


def test_process_exits_between_rounds_is_unconfirmable(fake):
    """巡检中途进程退出：单列 unconfirmable，其余 PID 照常报告。"""
    rows1 = [_close_wait_row(0, C1, 5001)]
    fake.add_pid(301, comm="victim", starttime=10, tcp_rows=rows1, fds={5: 5001})
    fake.add_pid(302, comm="survivor", starttime=20, tcp_rows=list(rows1), fds={9: 5001})

    inspector = ProcInspector(fake.root)
    r1 = inspector.snapshot_round([301, 302])
    assert all(s.identity is not None for s in r1.values())

    fake.remove_pid(301)  # 巡检途中退出
    result = _inspect_with_custom_rounds(inspector, [301, 302], r1)

    unconf = {u["pid"]: u for u in result.unconfirmable_pids}
    assert 301 in unconf
    assert "exited" in unconf[301]["reason"]
    assert unconf[301]["round1"]["comm"] == "victim"
    assert unconf[301]["round2"] is None

    # 存活的 302 照常出现在持有者里
    assert len(result.close_wait_connections) == 1
    holders = result.close_wait_connections[0]["holders_round2"]
    assert [h["pid"] for h in holders] == [302]


def _inspect_with_custom_rounds(inspector: ProcInspector, pids, round1):
    """复用已采的第一轮结果，第二轮实时采，模拟中途变化。"""
    import time

    unique = list(dict.fromkeys(pids))
    round2 = inspector.snapshot_round(unique)
    # 直接复用 inspect 的汇总逻辑：通过临时 monkeypatch snapshot_round
    calls = iter([round1, round2])
    inspector.snapshot_round = lambda p: next(calls)  # type: ignore[assignment]
    return inspector.inspect(unique, interval_seconds=0)


def test_pid_reuse_detected_via_starttime(fake):
    """PID 换代（同 PID、starttime 变了）必须进 unconfirmable。"""
    fake.add_pid(
        401, comm="old", starttime=100,
        tcp_rows=[_close_wait_row(0, C1, 6001)], fds={3: 6001},
    )
    inspector = ProcInspector(fake.root)
    r1 = inspector.snapshot_round([401])
    fake.replace_starttime(401, 999)  # 新进程复用了该 PID
    result = _inspect_with_custom_rounds(inspector, [401], r1)

    assert len(result.unconfirmable_pids) == 1
    u = result.unconfirmable_pids[0]
    assert u["pid"] == 401
    assert "reused" in u["reason"]
    assert u["round1"]["starttime"] == 100
    assert u["round2"]["starttime"] == 999
    # 换代的 PID 不得作为确认持有者
    assert result.close_wait_connections == []


def test_missing_pid_from_start_is_unconfirmable(fake):
    fake.add_pid(501, starttime=1, tcp_rows=[], fds={})
    result = ProcInspector(fake.root).inspect([501, 999], interval_seconds=0)
    unconf = {u["pid"]: u for u in result.unconfirmable_pids}
    assert 999 in unconf
    assert "not found" in unconf[999]["reason"]
    # 501 无连接，正常采样但没有连接输出
    assert result.close_wait_connections == []


def test_pid_missing_in_round1_but_present_round2(fake):
    """第一轮不存在、第二轮才出现（巡检中新启动）的 PID 不能作为确认持有者。"""
    inspector = ProcInspector(fake.root)
    r1 = inspector.snapshot_round([555])  # 此时 555 不存在
    assert r1[555].identity is None
    fake.add_pid(
        555, comm="late", starttime=7,
        tcp_rows=[_close_wait_row(0, C1, 5501)], fds={3: 5501},
    )
    result = _inspect_with_custom_rounds(inspector, [555], r1)
    assert [u["pid"] for u in result.unconfirmable_pids] == [555]
    assert "round 1 unreadable" in result.unconfirmable_pids[0]["reason"]
    assert result.close_wait_connections == []


def test_duplicate_pids_deduped_and_order_preserved(fake):
    fake.add_pid(601, starttime=1, tcp_rows=[], fds={})
    fake.add_pid(602, starttime=2, tcp_rows=[], fds={})
    result = ProcInspector(fake.root).inspect([602, 602, 601], interval_seconds=0)
    assert result.pids == [602, 601]


def test_non_socket_fds_ignored(fake, tmp_path):
    fake.add_pid(701, starttime=1, tcp_rows=[_close_wait_row(0, C1, 7001)], fds={3: 7001})
    # 额外挂一个非 socket FD
    os.symlink("/dev/null", os.path.join(fake.root, "701", "fd", "4"))
    result = ProcInspector(fake.root).inspect([701], interval_seconds=0)
    holders = result.close_wait_connections[0]["holders_round2"]
    assert holders[0]["fds"] == [3]


def test_round_summaries_record_failures(fake):
    fake.add_pid(801, starttime=1, tcp_rows=[_close_wait_row(0, C1, 8001)], fds={3: 8001})
    result = ProcInspector(fake.root).inspect([801, 802], interval_seconds=0)
    r1, r2 = result.rounds
    assert r1["sampled_pids"] == [801]
    assert {f["pid"] for f in r1["failed_pids"]} == {802}
    assert r1["close_wait_sockets_seen"] == 1

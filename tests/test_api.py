"""API 层测试：校验参数、线程池巡检、巡检期间服务可用性与演示编排。"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

import app.main as main_mod
from tests.fakeproc import FakeProc, tcp_row

CLOSE_WAIT = "08"


@pytest.fixture()
def fake_proc(tmp_path, monkeypatch):
    proc = FakeProc(str(tmp_path / "proc"))
    monkeypatch.setattr(main_mod, "PROC_ROOT", proc.root)
    return proc


@pytest.fixture()
def client():
    with TestClient(main_mod.app) as c:
        yield c


class FakeSampleHandler:
    """替代 httpx 调用 sample 服务的假处理器，记录调用。

    测试可通过替换 ``on_request`` 注入副作用（如模拟进程退出）。
    """

    def __init__(self):
        self.calls = []
        self.release_count = 0
        self.on_request = None

    async def __call__(self, method: str, url: str, **kwargs):
        class _Resp:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        self.calls.append((method, url, kwargs))
        if self.on_request is not None:
            await self.on_request(method, url)

        if url.endswith("/scenarios/shared-close-wait"):
            children = kwargs.get("params", {}).get("children", 2)
            pids = [900] + list(range(901, 901 + int(children)))
            return _Resp(
                {"scenario": "shared-close-wait", "pids": pids, "alive": True}
            )
        if url.endswith("/exit-during-inspection/setup"):
            return _Resp(
                {"scenario": "exit-during-inspection", "pids": [910], "alive": True}
            )
        if url.endswith("/exit-during-inspection/release"):
            self.release_count += 1
            return _Resp({"released": True})
        if url.endswith("/scenarios/status"):
            return _Resp({})
        if url.endswith("/scenarios"):
            return _Resp({"cleaned": True})
        raise AssertionError(f"unexpected url {url}")


def _make_fake_client(handler):
    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def request(self, method, url, **kwargs):
            return await handler(method, url, **kwargs)

        async def post(self, url, **kw):
            return await self.request("POST", url, **kw)

        async def get(self, url, **kw):
            return await self.request("GET", url, **kw)

        async def delete(self, url, **kw):
            return await self.request("DELETE", url, **kw)

    return _FakeClient


@pytest.fixture()
def fake_sample(monkeypatch):
    handler = FakeSampleHandler()
    monkeypatch.setattr(
        main_mod, "_sample_client", lambda: _make_fake_client(handler)()
    )
    return handler


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_inspect_validation_empty_pids(client):
    resp = client.post("/api/v1/inspect", json={"pids": [], "interval_seconds": 1})
    assert resp.status_code == 422


def test_inspect_validation_bad_interval(client):
    resp = client.post("/api/v1/inspect", json={"pids": [1], "interval_seconds": -1})
    assert resp.status_code == 422


def test_inspect_endpoint_end_to_end(client, fake_proc):
    row = tcp_row(0, "10.0.0.2:40000", "10.0.0.1:9000", CLOSE_WAIT, 3001)
    fake_proc.add_pid(101, starttime=11, tcp_rows=[row], fds={3: 3001, 8: 3001})
    resp = client.post(
        "/api/v1/inspect", json={"pids": [101], "interval_seconds": 0}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["pids"] == [101]
    assert len(body["close_wait_connections"]) == 1
    entry = body["close_wait_connections"][0]
    assert entry["holders_round2"][0]["fds"] == [3, 8]


def test_service_still_serves_during_long_inspection(client, fake_proc):
    """巡检间隔较长时，事件循环线程仍能立刻响应 /health。"""
    import threading

    fake_proc.add_pid(101, starttime=1, tcp_rows=[], fds={})
    latencies = []

    def run_inspection():
        start = time.monotonic()
        resp = client.post(
            "/api/v1/inspect", json={"pids": [101], "interval_seconds": 2}
        )
        latencies.append(time.monotonic() - start)
        assert resp.status_code == 200

    worker = threading.Thread(target=run_inspection)
    worker.start()
    time.sleep(0.5)  # 确保巡检已经进入两轮间隔

    health_start = time.monotonic()
    resp = client.get("/health")
    health_latency = time.monotonic() - health_start
    assert resp.status_code == 200
    worker.join()

    # 巡检整体耗时 >= 2s，但巡检期间的 health 请求应在毫秒级返回
    assert latencies[0] >= 2.0
    assert health_latency < 0.5


def test_demo_shared_close_wait(client, fake_proc, fake_sample):
    # sample 场景返回的 PID 需要在（假）proc 中存在并持有 CLOSE_WAIT
    pids = [900, 901]
    row = tcp_row(0, "10.0.0.2:50000", "10.0.0.1:9000", CLOSE_WAIT, 7001)
    for pid, fd in zip(pids, (4, 6)):
        fake_proc.add_pid(pid, comm="holder", starttime=pid, tcp_rows=[row], fds={fd: 7001})

    resp = client.post(
        "/demo/run-shared-close-wait",
        json={"interval_seconds": 0, "children": 1},
    )
    assert resp.status_code == 200
    body = resp.json()
    holders = body["inspection"]["close_wait_connections"][0]["holders_round2"]
    assert sorted(h["pid"] for h in holders) == [900, 901]


def test_demo_exit_during_inspection(client, fake_proc, fake_sample, monkeypatch):
    row = tcp_row(0, "10.0.0.2:50001", "10.0.0.1:9000", CLOSE_WAIT, 7002)
    fake_proc.add_pid(910, comm="holder", starttime=5, tcp_rows=[row], fds={4: 7002})

    async def on_request(method, url):
        # release 触发后，模拟 holder 从 /proc 消失
        if url.endswith("/release"):
            fake_proc.remove_pid(910)

    fake_sample.on_request = on_request

    # release 协程在 interval/2=0.5s 时触发；让第二轮采样前等待 0.8s，
    # 保证线程池中的第二轮读取发生在 release 之后
    from app.inspector import ProcInspector as _PI

    real_snapshot = _PI.snapshot_round
    call_count = {"n": 0}

    def patched_snapshot(self, pids):
        call_count["n"] += 1
        if call_count["n"] == 2:
            time.sleep(0.8)
        return real_snapshot(self, pids)

    monkeypatch.setattr(_PI, "snapshot_round", patched_snapshot)

    resp = client.post(
        "/demo/run-exit-during-inspection",
        json={"interval_seconds": 1, "children": 0},
    )
    assert resp.status_code == 200
    body = resp.json()
    unconf = body["inspection"]["unconfirmable_pids"]
    assert len(unconf) == 1
    assert unconf[0]["pid"] == 910
    assert "exited" in unconf[0]["reason"]
    assert fake_sample.release_count == 1


def test_demo_cleanup_and_status(client, fake_sample):
    assert client.delete("/demo/cleanup").status_code == 200
    assert client.get("/demo/status").status_code == 200

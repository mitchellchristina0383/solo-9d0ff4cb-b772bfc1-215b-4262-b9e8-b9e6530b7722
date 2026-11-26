"""API 层测试：校验参数、线程池巡检、巡检期间服务可用性与演示编排。"""

from __future__ import annotations

import threading
import time

import httpx
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

    按真实 sample 服务的语义维护"命名实例"：每次 setup 生成唯一实例名，
    release/status/cleanup 都只作用于对应实例，用于验证并发演示互不干扰。
    测试可通过替换 ``on_request`` 注入副作用（如模拟进程退出）。
    """

    def __init__(self):
        self.calls = []
        self.release_count = 0
        self.on_request = None
        # 实例名 -> {"pids": [...], "alive": bool}
        self.instances: dict = {}
        self.cleaned_instances: list = []
        # 置为 HTTP 状态码可让 release 强制失败
        self.release_fail_status: int | None = None
        self._seq = 0

    async def __call__(self, method: str, url: str, **kwargs):
        class _Resp:
            def __init__(self, payload, status_code=200, text=""):
                self._payload = payload
                self.status_code = status_code
                self.text = text or str(payload)

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise httpx.HTTPStatusError(
                        f"HTTP {self.status_code}",
                        request=httpx.Request("POST", url),
                        response=self,
                    )

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
            self._seq += 1
            name = f"exit-during-inspection-fake{self._seq}"
            self.instances[name] = {"pids": [910 + self._seq - 1], "alive": True}
            return _Resp(
                {
                    "scenario": name,
                    "pids": list(self.instances[name]["pids"]),
                    "alive": True,
                }
            )
        if "/exit-during-inspection/" in url and url.endswith("/release"):
            name = url.split("/exit-during-inspection/")[1].split("/")[0]
            inst = self.instances.get(name)
            if inst is None:
                return _Resp({"detail": "not found"}, status_code=404)
            if self.release_fail_status is not None:
                return _Resp(
                    {"detail": "forced failure"},
                    status_code=self.release_fail_status,
                )
            self.release_count += 1
            inst["alive"] = False
            return _Resp({"released": True, "scenario": name})
        if "/exit-during-inspection/" in url and url.endswith("/status"):
            name = url.split("/exit-during-inspection/")[1].split("/")[0]
            inst = self.instances.get(name)
            if inst is None:
                return _Resp({"detail": "not found"}, status_code=404)
            return _Resp({"scenario": name, "alive": inst["alive"]})
        if method == "DELETE" and "/exit-during-inspection/" in url:
            name = url.split("/exit-during-inspection/")[1]
            self.cleaned_instances.append(name)
            self.instances.pop(name, None)
            return _Resp({"cleaned": True, "scenario": name})
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


def _add_exiting_holder(fake_proc, pid=910, starttime=5, inode=7002):
    row = tcp_row(0, "10.0.0.2:50001", "10.0.0.1:9000", CLOSE_WAIT, inode)
    fake_proc.add_pid(pid, comm="holder", starttime=starttime, tcp_rows=[row], fds={4: inode})


def test_demo_exit_during_inspection(client, fake_proc, fake_sample, monkeypatch):
    """标准流程：首轮采到本次 holder 后才 release，确认退出后二采并出报告。"""
    monkeypatch.setattr(main_mod, "EXIT_CONFIRM_TIMEOUT", 2.0)
    _add_exiting_holder(fake_proc, pid=910)
    release_seen = {"t": None}
    round1_seen = {"t": None}
    started = time.monotonic()

    async def on_request(method, url):
        if url.endswith("/release"):
            release_seen["t"] = time.monotonic() - started
            # release 触发后，模拟本次 holder 从 /proc 消失
            fake_proc.remove_pid(910)

    fake_sample.on_request = on_request

    from app.inspector import ProcInspector as _PI

    real_snapshot = _PI.snapshot_round
    call_count = {"n": 0}

    def patched_snapshot(self, pids):
        call_count["n"] += 1
        if call_count["n"] == 1:
            round1_seen["t"] = time.monotonic() - started
        return real_snapshot(self, pids)

    monkeypatch.setattr(_PI, "snapshot_round", patched_snapshot)

    t0 = time.monotonic()
    resp = client.post(
        "/demo/run-exit-during-inspection",
        json={"interval_seconds": 1, "children": 0},
    )
    elapsed = time.monotonic() - t0
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # release 只发生一次，且晚于第一轮采样
    assert fake_sample.release_count == 1
    assert release_seen["t"] is not None and round1_seen["t"] is not None
    assert release_seen["t"] >= round1_seen["t"]
    # 完整睡满设定间隔后才进入第二轮（允许少量调度误差）
    assert elapsed >= 1.0

    unconf = body["inspection"]["unconfirmable_pids"]
    assert len(unconf) == 1
    assert unconf[0]["pid"] == 910
    assert "exited" in unconf[0]["reason"]
    assert unconf[0]["round1"] is not None
    assert unconf[0]["round2"] is None
    assert body["exit_confirmation"]["state"] == "exited"
    assert body["exit_confirmation"]["pid"] == 910


def test_demo_exit_concurrent_instances_isolated(
    client, fake_proc, fake_sample, monkeypatch
):
    """两个并发演示各自操作独立实例：release 只杀掉自己的 holder。"""
    monkeypatch.setattr(main_mod, "EXIT_CONFIRM_TIMEOUT", 2.0)
    _add_exiting_holder(fake_proc, pid=910, inode=7002)
    _add_exiting_holder(fake_proc, pid=911, inode=7003)

    async def on_request(method, url):
        # 每个实例的 release 只移除自己的 PID
        if url.endswith("/release"):
            name = url.split("/exit-during-inspection/")[1].split("/")[0]
            pid = fake_sample.instances[name]["pids"][0]
            fake_proc.remove_pid(pid)

    fake_sample.on_request = on_request

    results = []
    errors = []

    def run_demo():
        try:
            local = TestClient(main_mod.app)
            resp = local.post(
                "/demo/run-exit-during-inspection",
                json={"interval_seconds": 1, "children": 0},
            )
            results.append(resp)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=run_demo)
    t2 = threading.Thread(target=run_demo)
    t1.start()
    # 第二个请求在第一个等待第二轮采样期间就开始
    time.sleep(0.3)
    t2.start()
    t1.join(timeout=15)
    t2.join(timeout=15)

    assert not errors
    assert len(results) == 2
    instances = []
    for resp in results:
        assert resp.status_code == 200, resp.text
        body = resp.json()
        instances.append(body["scenario"]["scenario"])
        unconf = body["inspection"]["unconfirmable_pids"]
        assert len(unconf) == 1
        assert unconf[0]["pid"] == body["scenario"]["pids"][0]
        assert body["exit_confirmation"]["state"] == "exited"
    # 两次 setup 产生了不同的实例名，各释放各的
    assert len(set(instances)) == 2
    assert fake_sample.release_count == 2


def test_demo_exit_round1_missing_returns_error_and_cleans_up(
    client, fake_proc, fake_sample, monkeypatch
):
    """首轮采样排队期间 holder 已退出：返回错误而非假成功，并回收本次实例。"""
    monkeypatch.setattr(main_mod, "EXIT_CONFIRM_TIMEOUT", 2.0)
    # 故意不向 fake_proc 添加 910：模拟首轮就采不到本次进程
    resp = client.post(
        "/demo/run-exit-during-inspection",
        json={"interval_seconds": 1, "children": 0},
    )
    assert resp.status_code == 502
    assert "round 1 sampling failed" in resp.json()["detail"]
    # 未触发 release，且本次实例已被回收
    assert fake_sample.release_count == 0
    assert len(fake_sample.cleaned_instances) == 1


def test_demo_exit_release_failure_returns_error_and_cleans_up(
    client, fake_proc, fake_sample, monkeypatch
):
    """release 被 sample 拒绝：返回错误并回收本次 holder。"""
    monkeypatch.setattr(main_mod, "EXIT_CONFIRM_TIMEOUT", 2.0)
    _add_exiting_holder(fake_proc, pid=910)
    fake_sample.release_fail_status = 409

    resp = client.post(
        "/demo/run-exit-during-inspection",
        json={"interval_seconds": 1, "children": 0},
    )
    assert resp.status_code == 502
    assert "release rejected" in resp.json()["detail"]
    assert len(fake_sample.cleaned_instances) == 1


def test_demo_exit_holder_exits_before_release(
    client, fake_proc, fake_sample, monkeypatch
):
    """首轮采到后、release 时进程已自行退出（409）：报错并回收，不假装成功。"""
    monkeypatch.setattr(main_mod, "EXIT_CONFIRM_TIMEOUT", 2.0)
    _add_exiting_holder(fake_proc, pid=910)
    fake_sample.release_fail_status = 409

    resp = client.post(
        "/demo/run-exit-during-inspection",
        json={"interval_seconds": 1, "children": 0},
    )
    assert resp.status_code == 502
    assert "release rejected" in resp.json()["detail"]
    assert fake_sample.cleaned_instances  # 已尝试回收


def test_demo_exit_pid_reuse_detected(client, fake_proc, fake_sample, monkeypatch):
    """release 后同 PID 被新进程换代：识别为 reused 并列入 unconfirmable。"""
    monkeypatch.setattr(main_mod, "EXIT_CONFIRM_TIMEOUT", 2.0)
    _add_exiting_holder(fake_proc, pid=910, starttime=5)

    async def on_request(method, url):
        if url.endswith("/release"):
            # 模拟同 PID 被新进程占用（starttime 变化）
            fake_proc.replace_starttime(910, 999)

    fake_sample.on_request = on_request

    resp = client.post(
        "/demo/run-exit-during-inspection",
        json={"interval_seconds": 1, "children": 0},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["exit_confirmation"]["state"] == "reused"
    unconf = body["inspection"]["unconfirmable_pids"]
    assert len(unconf) == 1
    assert unconf[0]["pid"] == 910
    assert "reused" in unconf[0]["reason"]


def test_demo_exit_full_interval_waited_before_round2(
    client, fake_proc, fake_sample, monkeypatch
):
    """第二轮必须在睡满完整间隔且确认退出之后才开始。"""
    monkeypatch.setattr(main_mod, "EXIT_CONFIRM_TIMEOUT", 2.0)
    _add_exiting_holder(fake_proc, pid=910)
    timeline = []

    async def on_request(method, url):
        if url.endswith("/release"):
            timeline.append(("release", time.monotonic()))
            fake_proc.remove_pid(910)

    fake_sample.on_request = on_request

    from app.inspector import ProcInspector as _PI

    real_snapshot = _PI.snapshot_round

    def patched_snapshot(self, pids):
        timeline.append((f"snapshot#{len(timeline)}", time.monotonic()))
        return real_snapshot(self, pids)

    monkeypatch.setattr(_PI, "snapshot_round", patched_snapshot)

    t0 = time.monotonic()
    resp = client.post(
        "/demo/run-exit-during-inspection",
        json={"interval_seconds": 1, "children": 0},
    )
    assert resp.status_code == 200, resp.text
    release_t = next(t for name, t in timeline if name == "release")
    second_snapshot_t = timeline[-1][1]
    # 第二次快照距 release 至少隔了完整 interval（确认退出轮询在其后）
    assert second_snapshot_t - release_t >= 1.0 - 0.05
    assert second_snapshot_t - t0 >= 1.0


def test_demo_cleanup_and_status(client, fake_sample):
    assert client.delete("/demo/cleanup").status_code == 200
    assert client.get("/demo/status").status_code == 200

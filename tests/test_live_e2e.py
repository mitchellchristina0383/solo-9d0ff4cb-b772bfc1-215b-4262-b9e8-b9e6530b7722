"""真实进程端到端测试：本地拉起 sample + api 两个 uvicorn 服务。

不依赖 Docker（本环境无 Docker 守护进程），但使用与容器内完全相同的
应用代码。Docker Compose 场景仅多一层 PID 命名空间共享，proc 读取逻辑不变。

运行：pytest tests/test_live_e2e.py -m live
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time

import httpx
import pytest

pytestmark = pytest.mark.live

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
API_PORT = 18000
SAMPLE_PORT = 18001


def _wait_healthy(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        try:
            resp = httpx.get(url, timeout=2)
            if resp.status_code == 200:
                return
        except httpx.HTTPError as exc:
            last_exc = exc
        time.sleep(0.3)
    raise RuntimeError(f"service never became healthy: {url}: {last_exc}")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_services():
    api_port = _free_port()
    sample_port = _free_port()
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT
    env["SAMPLE_BASE_URL"] = f"http://127.0.0.1:{sample_port}"

    sample_proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "sample_app:app",
            "--app-dir", os.path.join(ROOT, "sample"),
            "--host", "127.0.0.1", "--port", str(sample_port),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    api_proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--app-dir", ROOT,
            "--host", "127.0.0.1", "--port", str(api_port),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_healthy(f"http://127.0.0.1:{sample_port}/health")
        _wait_healthy(f"http://127.0.0.1:{api_port}/health")
        yield f"http://127.0.0.1:{api_port}"
    finally:
        for proc in (api_proc, sample_proc):
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


def test_shared_close_wait_real_processes(live_services):
    base = live_services
    httpx.delete(f"{base}/demo/cleanup", timeout=10)
    resp = httpx.post(
        f"{base}/demo/run-shared-close-wait",
        json={"interval_seconds": 1, "children": 2},
        timeout=30,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # 场景侧：1 父 + 2 子，共 3 个 PID
    assert len(body["scenario"]["pids"]) == 3
    inspection = body["inspection"]
    assert inspection["unconfirmable_pids"] == []
    assert len(inspection["close_wait_connections"]) >= 1

    entry = inspection["close_wait_connections"][0]
    assert entry["connection"]["state"] == "CLOSE_WAIT"
    assert entry["inode_stable"] is True
    holder_pids = sorted(h["pid"] for h in entry["holders_round2"])
    assert holder_pids == sorted(body["scenario"]["pids"])
    # 每个持有者都应报告具体 FD
    for holder in entry["holders_round2"]:
        assert holder["fds"], f"pid {holder['pid']} missing fds"

    httpx.delete(f"{base}/demo/cleanup", timeout=10)


def test_exit_during_inspection_real_process(live_services):
    base = live_services
    httpx.delete(f"{base}/demo/cleanup", timeout=10)
    resp = httpx.post(
        f"{base}/demo/run-exit-during-inspection",
        json={"interval_seconds": 2, "children": 0},
        timeout=30,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    unconf = body["inspection"]["unconfirmable_pids"]
    assert len(unconf) == 1
    assert unconf[0]["pid"] == body["scenario"]["pids"][0]
    assert "exited" in unconf[0]["reason"]
    # 第一轮采到过该进程，第二轮已消失
    assert unconf[0]["round1"] is not None
    assert unconf[0]["round2"] is None

    httpx.delete(f"{base}/demo/cleanup", timeout=10)


def test_raw_inspect_with_unknown_pid(live_services):
    base = live_services
    resp = httpx.post(
        f"{base}/api/v1/inspect",
        json={"pids": [999999], "interval_seconds": 0},
        timeout=15,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["unconfirmable_pids"][0]["pid"] == 999999
    assert body["close_wait_connections"] == []

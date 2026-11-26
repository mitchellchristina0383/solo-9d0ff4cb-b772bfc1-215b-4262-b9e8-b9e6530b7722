"""样例场景控制服务（运行在 sample 容器内，监听 8001）。

负责：
- 启动 closer 端（本地 9000）；
- 按场景拉起 holder 子进程并管理生命周期；
- 提供场景状态查询、清理，以及"巡检中退出"的 release 信号。

并发模型
--------
每次演示请求都生成唯一的场景实例名（``exit-during-inspection-<uuid>``），
holder 进程、状态文件与实例登记项都按该名称隔离：

- 一个请求的 release/cleanup 只会命中自己 setup 出来的进程，
  不会影响其他并发演示；
- 状态文件路径同样带实例名，避免互相覆盖读到旧状态。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException

from closer import start_closer

STATE_DIR = os.environ.get("SAMPLE_STATE_DIR", "/tmp/sample-state")
HOLDER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "holder.py")
CLOSER_PORT = int(os.environ.get("CLOSER_PORT", "9000"))

# holder 建连并写回状态的最长等待时间
STATUS_WAIT_SECONDS = 5.0
# release 后等 holder 自行退出的最长时间
RELEASE_WAIT_SECONDS = 5.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_closer(port=CLOSER_PORT)
    os.makedirs(STATE_DIR, exist_ok=True)
    yield
    # 服务关闭时清理所有 holder 进程组，避免容器里留下孤儿
    for name in list(_processes):
        _kill(name)


app = FastAPI(
    title="CLOSE_WAIT 样例场景服务", version="1.1.0", lifespan=lifespan
)

# 场景实例名 -> holder 子进程对象（并发演示各自独立的 key）
_processes: Dict[str, subprocess.Popen] = {}
# 场景实例名 -> 最近一次 holder 写回的状态
_status_cache: Dict[str, dict] = {}


def _status_path(name: str) -> str:
    return os.path.join(STATE_DIR, f"{name}.json")


def _read_status(name: str) -> Optional[dict]:
    path = _status_path(name)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def _new_instance_name(scenario: str) -> str:
    return f"{scenario}-{uuid.uuid4().hex}"


def _spawn_holder(name: str, mode: str, extra_args: List[str]) -> dict:
    os.makedirs(STATE_DIR, exist_ok=True)
    # 该实例名是新生成的 uuid，正常不会有残留；防御性清理一次
    try:
        os.unlink(_status_path(name))
    except FileNotFoundError:
        pass

    proc = subprocess.Popen(
        [
            sys.executable,
            HOLDER_PATH,
            "--mode",
            mode,
            "--closer-port",
            str(CLOSER_PORT),
            "--status-file",
            _status_path(name),
            *extra_args,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    _processes[name] = proc

    # holder 建连并写状态通常在几十毫秒内，最多等 STATUS_WAIT_SECONDS
    deadline = time.time() + STATUS_WAIT_SECONDS
    status = None
    while time.time() < deadline:
        status = _read_status(name)
        if status is not None:
            break
        if proc.poll() is not None:
            _processes.pop(name, None)
            raise HTTPException(
                500, f"holder process exited early (rc={proc.returncode})"
            )
        time.sleep(0.1)
    if status is None:
        _kill(name)
        raise HTTPException(504, "holder did not report status in time")
    _status_cache[name] = status
    return {"scenario": name, **status, "alive": proc.poll() is None}


def _kill(name: str) -> None:
    """只回收指定实例自己的 holder 进程组。"""
    proc = _processes.pop(name, None)
    if proc is None:
        _status_cache.pop(name, None)
        return
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
    _status_cache.pop(name, None)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/scenarios/shared-close-wait")
def create_shared(children: int = 2, duration: float = 120.0) -> dict:
    # 共享场景同一时刻只服务一个演示名；先清掉同名旧实例（互不影响其他实例）
    _kill("shared-close-wait")
    return _spawn_holder(
        "shared-close-wait",
        "shared",
        ["--children", str(children), "--duration", str(duration)],
    )


@app.post("/scenarios/exit-during-inspection/setup")
def setup_exit() -> dict:
    """每次调用生成独立实例，并发演示互不干扰。"""
    name = _new_instance_name("exit-during-inspection")
    return _spawn_holder(name, "exiting", [])


@app.post("/scenarios/exit-during-inspection/{name}/release")
def release_exit(name: str) -> dict:
    """只释放指定实例的 holder；进程不属于本调用方时返回 404。"""
    proc = _processes.get(name)
    if proc is None:
        raise HTTPException(404, f"scenario instance not found: {name}")
    if proc.poll() is not None:
        # 已退出（例如 holder 的安全兜底到期）：登记移除并如实回报
        _processes.pop(name, None)
        raise HTTPException(409, f"scenario instance already exited: {name}")
    try:
        os.kill(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        _processes.pop(name, None)
        raise HTTPException(409, f"scenario instance already exited: {name}")
    try:
        proc.wait(timeout=RELEASE_WAIT_SECONDS)
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "holder did not exit after SIGTERM")
    _processes.pop(name, None)
    status = _read_status(name) or {}
    _status_cache[name] = status
    return {"released": True, "scenario": name, "final_status": status}


@app.get("/scenarios/exit-during-inspection/{name}/status")
def exit_instance_status(name: str) -> dict:
    """查询单个实例：holder 是否仍在登记中且进程仍然存活。"""
    proc = _processes.get(name)
    if proc is None:
        return {"scenario": name, "alive": False, "status": _read_status(name)}
    alive = proc.poll() is None
    if not alive:
        # 进程自行退出（兜底超时等），同步清理登记，避免假"存活"
        _processes.pop(name, None)
    return {
        "scenario": name,
        "alive": alive,
        "status": _read_status(name) or _status_cache.get(name),
    }


@app.delete("/scenarios/exit-during-inspection/{name}")
def cleanup_exit_instance(name: str) -> dict:
    """回收指定实例；不存在视为已清理，保证调用方可安全重试。"""
    _kill(name)
    return {"cleaned": True, "scenario": name}


@app.get("/scenarios/status")
def scenarios_status() -> dict:
    result = {}
    for name, proc in list(_processes.items()):
        result[name] = {
            "alive": proc.poll() is None,
            "status": _read_status(name) or _status_cache.get(name),
        }
    return result


@app.delete("/scenarios")
def cleanup() -> dict:
    for name in list(_processes):
        _kill(name)
    return {"cleaned": True}

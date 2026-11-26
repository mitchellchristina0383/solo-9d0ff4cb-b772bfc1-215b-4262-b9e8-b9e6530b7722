"""样例场景控制服务（运行在 sample 容器内，监听 8001）。

负责：
- 启动 closer 端（本地 9000）；
- 按场景拉起 holder 子进程并管理生命周期；
- 提供场景状态查询、清理，以及"巡检中退出"的 release 信号。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException

from closer import start_closer

STATE_DIR = os.environ.get("SAMPLE_STATE_DIR", "/tmp/sample-state")
HOLDER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "holder.py")
CLOSER_PORT = int(os.environ.get("CLOSER_PORT", "9000"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    start_closer(port=CLOSER_PORT)
    os.makedirs(STATE_DIR, exist_ok=True)
    yield
    # 服务关闭时清理所有 holder 进程组，避免容器里留下孤儿
    for name in list(_processes):
        _kill(name)


app = FastAPI(
    title="CLOSE_WAIT 样例场景服务", version="1.0.0", lifespan=lifespan
)

# 场景名 -> holder 子进程对象
_processes: Dict[str, subprocess.Popen] = {}
# 场景名 -> 最近一次 holder 写回的状态
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


def _spawn_holder(name: str, extra_args: List[str]) -> dict:
    os.makedirs(STATE_DIR, exist_ok=True)
    # 上一轮残留先清掉，避免读到旧状态
    try:
        os.unlink(_status_path(name))
    except FileNotFoundError:
        pass

    proc = subprocess.Popen(
        [
            sys.executable,
            HOLDER_PATH,
            "--mode",
            _mode_of(name),
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

    # holder 建连并写状态通常在几十毫秒内，最多等 5 秒
    deadline = time.time() + 5
    status = None
    while time.time() < deadline:
        status = _read_status(name)
        if status is not None:
            break
        if proc.poll() is not None:
            _processes.pop(name, None)
            raise HTTPException(500, f"holder process exited early (rc={proc.returncode})")
        time.sleep(0.1)
    if status is None:
        raise HTTPException(504, "holder did not report status in time")
    _status_cache[name] = status
    return {"scenario": name, **status, "alive": proc.poll() is None}


def _mode_of(scenario: str) -> str:
    return {
        "shared-close-wait": "shared",
        "exit-during-inspection": "exiting",
    }[scenario]


def _kill(name: str) -> None:
    proc = _processes.pop(name, None)
    if proc is None:
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
    _kill("shared-close-wait")
    return _spawn_holder(
        "shared-close-wait",
        ["--children", str(children), "--duration", str(duration)],
    )


@app.post("/scenarios/exit-during-inspection/setup")
def setup_exit() -> dict:
    _kill("exit-during-inspection")
    return _spawn_holder("exit-during-inspection", [])


@app.post("/scenarios/exit-during-inspection/release")
def release_exit() -> dict:
    proc = _processes.get("exit-during-inspection")
    if proc is None or proc.poll() is not None:
        raise HTTPException(409, "exit-during-inspection scenario not running")
    os.kill(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "holder did not exit after SIGTERM")
    _processes.pop("exit-during-inspection", None)
    status = _read_status("exit-during-inspection") or {}
    _status_cache["exit-during-inspection"] = status
    return {"released": True, "final_status": status}


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

"""巡检 API 服务。

端点
----
- GET  /health                       存活检查（巡检期间也能响应）
- POST /api/v1/inspect               提交 PID 列表与采样间隔，执行两轮 CLOSE_WAIT 巡检
- POST /demo/run-shared-close-wait   一键演示：多个 PID 持有同一 CLOSE_WAIT 连接
- POST /demo/run-exit-during-inspection 一键演示：巡检途中进程退出
- GET  /demo/status / DELETE /demo/cleanup  演示场景管理（转发到 sample 服务）

巡检是同步 /proc IO，放在线程池执行，事件循环不被阻塞，
巡检期间服务照常接入其他请求（见 tests/test_api.py 的并发用例）。
"""

from __future__ import annotations

import asyncio
import os
from typing import List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from .inspector import ProcInspector

SAMPLE_BASE_URL = os.environ.get("SAMPLE_BASE_URL", "http://sample:8001")
PROC_ROOT = os.environ.get("PROC_ROOT", "/proc")

app = FastAPI(
    title="CLOSE_WAIT 巡检 API",
    description=(
        "提交一组 PID 和采样间隔，从 Linux /proc 采两轮 IPv4 TCP 状态与套接字归属，"
        "输出两轮均为 CLOSE_WAIT 的连接，以及第二轮仍持有它的 PID/FD。"
        "中途退出或 PID 换代的进程列入 unconfirmable_pids。"
    ),
    version="1.0.0",
)


class InspectRequest(BaseModel):
    pids: List[int] = Field(
        ...,
        min_length=1,
        max_length=128,
        description="受查 PID 列表（自动去重，保持给定顺序）",
    )
    interval_seconds: float = Field(
        1.0, ge=0.0, le=60.0, description="两轮采样之间的间隔秒数"
    )


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "proc_root": PROC_ROOT}


@app.post("/api/v1/inspect", tags=["inspect"])
async def inspect(req: InspectRequest) -> dict:
    for pid in req.pids:
        if pid <= 0:
            raise HTTPException(status_code=422, detail=f"invalid pid: {pid}")
    inspector = ProcInspector(proc_root=PROC_ROOT)
    result = await run_in_threadpool(
        inspector.inspect, req.pids, req.interval_seconds
    )
    return result.to_dict()


# --------------------------------------------------------------------------- #
# 演示编排：转发给 sample 容器里的场景服务，并在本服务内驱动完整巡检流程
# --------------------------------------------------------------------------- #
def _sample_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=SAMPLE_BASE_URL, timeout=30.0)


class SharedDemoRequest(BaseModel):
    interval_seconds: float = Field(1.0, ge=0.0, le=60.0)
    children: int = Field(1, ge=0, le=4, description="共享连接的 fork 子进程数（不含父进程）")


class ExitDemoRequest(BaseModel):
    interval_seconds: float = Field(3.0, ge=1.0, le=30.0)
    children: int = Field(1, ge=0, le=4, description="兼容字段，退出场景不使用")


@app.post("/demo/run-shared-close-wait", tags=["demo"])
async def demo_shared_close_wait(req: SharedDemoRequest) -> dict:
    """一键：在 sample 中制造被多个 PID 同时持有的 CLOSE_WAIT 连接，再巡检。

    holder 父进程建立连接后 fork，子进程继承同一个 socket FD，
    因此同一连接（同一 inode）会出现在多个 PID 的 FD 列表里。
    """
    async with _sample_client() as client:
        setup = await client.post(
            "/scenarios/shared-close-wait",
            params={"children": req.children, "duration": 120},
        )
        setup.raise_for_status()
        scenario = setup.json()

    inspector = ProcInspector(proc_root=PROC_ROOT)
    result = await run_in_threadpool(
        inspector.inspect, scenario["pids"], req.interval_seconds
    )
    return {"scenario": scenario, "inspection": result.to_dict()}


@app.post("/demo/run-exit-during-inspection", tags=["demo"])
async def demo_exit_during_inspection(req: ExitDemoRequest) -> dict:
    """一键：巡检途中一个持有 CLOSE_WAIT 的进程退出。

    sample 侧的待退进程阻塞等待 release 信号；本服务在两轮采样之间
    （间隔过半时）发信号让它退出，第二轮采样即发现它消失，
    进入 unconfirmable_pids，其余 PID 照常出报告。
    """
    interval = req.interval_seconds
    async with _sample_client() as client:
        setup = await client.post(
            "/scenarios/exit-during-inspection/setup"
        )
        setup.raise_for_status()
        scenario = setup.json()

        async def release_at_half_time() -> None:
            await asyncio.sleep(interval / 2.0)
            try:
                await client.post(
                    "/scenarios/exit-during-inspection/release"
                )
            except httpx.HTTPError:
                pass

        release_task = asyncio.create_task(release_at_half_time())
        try:
            inspector = ProcInspector(proc_root=PROC_ROOT)
            result = await run_in_threadpool(
                inspector.inspect, scenario["pids"], interval
            )
        finally:
            release_task.cancel()
        return {"scenario": scenario, "inspection": result.to_dict()}


@app.get("/demo/status", tags=["demo"])
async def demo_status() -> dict:
    async with _sample_client() as client:
        resp = await client.get("/scenarios/status")
        resp.raise_for_status()
        return resp.json()


@app.delete("/demo/cleanup", tags=["demo"])
async def demo_cleanup() -> dict:
    async with _sample_client() as client:
        resp = await client.delete("/scenarios")
        resp.raise_for_status()
        return resp.json()

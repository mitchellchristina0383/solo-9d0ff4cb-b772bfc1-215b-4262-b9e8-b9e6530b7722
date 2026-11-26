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
from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .inspector import ProcInspector

SAMPLE_BASE_URL = os.environ.get("SAMPLE_BASE_URL", "http://sample:8001")
PROC_ROOT = os.environ.get("PROC_ROOT", "/proc")

# release 后等待本实例 holder 真正退出的上限
EXIT_CONFIRM_TIMEOUT = float(os.environ.get("EXIT_CONFIRM_TIMEOUT", "10.0"))
# 退出确认轮询间隔
EXIT_CONFIRM_POLL = 0.1

app = FastAPI(
    title="CLOSE_WAIT 巡检 API",
    description=(
        "提交一组 PID 和采样间隔，从 Linux /proc 采两轮 IPv4 TCP 状态与套接字归属，"
        "输出两轮均为 CLOSE_WAIT 的连接，以及第二轮仍持有它的 PID/FD。"
        "中途退出或 PID 换代的进程列入 unconfirmable_pids。"
    ),
    version="1.1.0",
)


class DemoError(Exception):
    """演示编排失败：携带 HTTP 状态码与面向调用方的错误明细。"""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@app.exception_handler(DemoError)
async def _demo_error_handler(_: Request, exc: DemoError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


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


def _pid_alive_in_proc(pid: int, expected_starttime: Optional[int]) -> str:
    """检查目标 PID 在共享 /proc 中的真实状态。

    返回 ``"alive"`` / ``"exited"`` / ``"reused"``。
    读不到 stat 视为已退出（进程消失或无权访问都意味着本次无法再确认它）。
    能读到但 starttime 与首轮不符，说明 PID 已被别的进程换代。
    """
    stat_path = os.path.join(PROC_ROOT, str(pid), "stat")
    try:
        with open(stat_path, "r", encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except FileNotFoundError:
        return "exited"
    except (PermissionError, ProcessLookupError, OSError):
        return "exited"
    rparen = text.rfind(")")
    if rparen == -1:
        return "exited"
    fields = text[rparen + 2 :].split()
    if len(fields) < 20:
        return "exited"
    try:
        starttime = int(fields[19])
    except ValueError:
        return "exited"
    if expected_starttime is not None and starttime != expected_starttime:
        return "reused"
    return "alive"


async def _wait_until_pid_exited(
    client: httpx.AsyncClient,
    instance: str,
    pid: int,
    expected_starttime: Optional[int],
    timeout: float,
) -> str:
    """等待本次实例的 holder 退出。

    双重确认：sample 实例登记状态 + 共享 /proc 中的进程身份。
    返回最终状态（``"exited"`` 或 ``"reused"``）；超时抛 DemoError。
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        # 1) sample 侧登记：该实例是否还活着
        sample_alive: Optional[bool]
        try:
            resp = await client.get(
                f"/scenarios/exit-during-inspection/{instance}/status"
            )
            if resp.status_code == 404:
                sample_alive = False
            elif resp.status_code >= 400:
                raise DemoError(
                    502,
                    f"sample status check failed for {instance}: "
                    f"HTTP {resp.status_code}",
                )
            else:
                sample_alive = bool(resp.json().get("alive"))
        except httpx.HTTPError as exc:
            raise DemoError(502, f"sample status check failed for {instance}: {exc}")

        # 2) /proc 侧的真实进程身份
        proc_state = await run_in_threadpool(
            _pid_alive_in_proc, pid, expected_starttime
        )

        if proc_state == "reused":
            return "reused"
        if proc_state == "exited" and not sample_alive:
            return "exited"

        if asyncio.get_running_loop().time() >= deadline:
            raise DemoError(
                504,
                f"holder pid {pid} of instance {instance} did not exit within "
                f"{timeout:.0f}s after release (sample_alive={sample_alive}, "
                f"proc_state={proc_state})",
            )
        await asyncio.sleep(EXIT_CONFIRM_POLL)


@app.post("/demo/run-exit-during-inspection", tags=["demo"])
async def demo_exit_during_inspection(req: ExitDemoRequest) -> dict:
    """一键：巡检途中**本次演示自己启动的** holder 进程退出。

    并发安全与时序保证
    ------------------
    1. 每次调用向 sample 申请一个独立命名实例，只持有/释放/回收自己的
       holder 进程；两个并发演示互不干扰，第二个请求无需等待第一个结束，
       可以在第一个等待第二轮采样期间直接开始。
    2. 先做第一轮采样并确认确实采到了本次 holder（身份与 CLOSE_WAIT 连接
       均在），随后才发 release；首轮未采到则报错并回收，绝不"假成功"。
    3. release 之后先睡满调用方设定的完整间隔，再确认本次 holder 已退出
       （sample 登记 + /proc 身份双重确认，识别 PID 换代），然后才开始
       第二轮采样。
    4. 采样或退出任一步失败都返回错误，并尽力回收本次实例的 holder。
    """
    interval = req.interval_seconds
    instance: Optional[str] = None
    scenario: Optional[dict] = None

    async def _cleanup_instance() -> None:
        if instance is None:
            return
        try:
            await client.delete(
                f"/scenarios/exit-during-inspection/{instance}"
            )
        except httpx.HTTPError:
            pass  # 回收是尽力而为：sample 侧进程组也有生命周期兜底

    async with _sample_client() as client:
        # ---- 步骤 0：申请本次演示独占的场景实例 ------------------------- #
        try:
            setup = await client.post(
                "/scenarios/exit-during-inspection/setup"
            )
            setup.raise_for_status()
            scenario = setup.json()
        except httpx.HTTPError as exc:
            raise DemoError(502, f"scenario setup failed: {exc}")
        instance = str(scenario["scenario"])
        pids = list(dict.fromkeys(scenario["pids"]))

        try:
            inspector = ProcInspector(proc_root=PROC_ROOT)

            # ---- 步骤 1：第一轮采样，必须先采到本次 holder -------------- #
            round1 = await run_in_threadpool(inspector.snapshot_round, pids)
            target_pid = pids[0]
            snap1 = round1.get(target_pid)
            if snap1 is None or snap1.identity is None:
                reason = snap1.error if snap1 is not None else "snapshot missing"
                raise DemoError(
                    502,
                    f"round 1 sampling failed for this demo's holder pid "
                    f"{target_pid}: {reason}",
                )
            expected_starttime = snap1.identity.starttime
            saw_close_wait = any(
                conn.state == "08" for conn in snap1.connections.values()
            )
            if not saw_close_wait:
                raise DemoError(
                    502,
                    f"round 1 sampled holder pid {target_pid} but no CLOSE_WAIT "
                    "socket was observed; aborting before release",
                )

            # ---- 步骤 2：首轮确认后才让本次 holder 退出 ---------------- #
            try:
                rel = await client.post(
                    f"/scenarios/exit-during-inspection/{instance}/release"
                )
            except httpx.HTTPError as exc:
                raise DemoError(
                    502, f"release failed for instance {instance}: {exc}"
                )
            if rel.status_code >= 400:
                raise DemoError(
                    502,
                    f"release rejected for instance {instance}: "
                    f"HTTP {rel.status_code} {rel.text}",
                )

            # ---- 步骤 3：睡满完整设定间隔 ------------------------------ #
            await asyncio.sleep(interval)

            # ---- 步骤 4：双重确认本次 holder 确已退出 ------------------ #
            exit_state = await _wait_until_pid_exited(
                client,
                instance,
                target_pid,
                expected_starttime,
                EXIT_CONFIRM_TIMEOUT,
            )

            # ---- 步骤 5：第二轮采样 ------------------------------------ #
            round2 = await run_in_threadpool(inspector.snapshot_round, pids)

            # 一致性校验：确认退出后第二轮不应再看到同一身份的进程；
            # 若为 reused（PID 换代），build_result 会凭 starttime 变化列入 unconfirmable
            snap2 = round2.get(target_pid)
            if (
                exit_state == "exited"
                and snap2 is not None
                and snap2.identity is not None
            ):
                raise DemoError(
                    500,
                    f"holder pid {target_pid} confirmed exited but reappeared "
                    "in round 2 sampling",
                )

            result = await run_in_threadpool(
                inspector.build_result, pids, interval, round1, round2
            )
            return {
                "scenario": scenario,
                "exit_confirmation": {
                    "pid": target_pid,
                    "state": exit_state,
                    "interval_seconds_waited": interval,
                },
                "inspection": result.to_dict(),
            }
        except DemoError:
            await _cleanup_instance()
            raise
        except Exception as exc:  # noqa: BLE001 - 任何意外都要回收本次样例
            await _cleanup_instance()
            raise DemoError(500, f"exit-during-inspection demo failed: {exc}")


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

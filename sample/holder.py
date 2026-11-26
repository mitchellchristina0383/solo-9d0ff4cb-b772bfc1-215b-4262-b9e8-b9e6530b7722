"""样例 holder 进程：由 sample_app 以独立子进程方式拉起。

两种模式
--------
shared:
    连接 closer 后 fork 出 N 个子进程，父子进程共同持有同一个 socket FD
    （同一 inode 出现在多个 PID 下）；closer 主动断开后，该连接在所有
    holder 上都进入 CLOSE_WAIT，直到 duration 到期进程退出。

exiting:
    同样持有一条 CLOSE_WAIT 连接，但阻塞等待 SIGTERM；
    巡检 API 在两轮采样之间发信号，让它在巡检中途退出。

状态以 JSON 文件原子写回 sample_app 约定的路径。
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import tempfile
import time


def write_status(path: str, payload: dict) -> None:
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".status-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.rename(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def connect_close_wait(closer_host: str, closer_port: int) -> socket.socket:
    """连上 closer 并等对端关闭，本端不 close -> CLOSE_WAIT。"""
    sock = socket.create_connection((closer_host, closer_port), timeout=10)
    # 触发对端处理后关闭；本端保持 FD 打开
    sock.sendall(b"ping\n")
    deadline = time.time() + 10
    while time.time() < deadline:
        # 对端 close 后 recv 返回 b""；此时内核已把连接置为 CLOSE_WAIT
        chunk = sock.recv(16)
        if chunk == b"":
            break
        time.sleep(0.05)
    return sock


def run_shared(args: argparse.Namespace) -> int:
    sock = connect_close_wait(args.closer_host, args.closer_port)
    pids = [os.getpid()]
    for _ in range(args.children):
        child = os.fork()
        if child == 0:
            # 子进程继承 sock，什么都不做，安静睡满时长
            time.sleep(args.duration)
            os._exit(0)
        pids.append(child)

    write_status(
        args.status_file,
        {
            "mode": "shared",
            "pids": pids,
            "fd": sock.fileno(),
            "closer": f"{args.closer_host}:{args.closer_port}",
        },
    )
    time.sleep(args.duration)
    return 0


def run_exiting(args: argparse.Namespace) -> int:
    sock = connect_close_wait(args.closer_host, args.closer_port)

    def _term(signum, frame):  # noqa: ARG001
        write_status(
            args.status_file,
            {
                "mode": "exiting",
                "pids": [os.getpid()],
                "fd": sock.fileno(),
                "released": True,
            },
        )
        sys.exit(0)

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    write_status(
        args.status_file,
        {
            "mode": "exiting",
            "pids": [os.getpid()],
            "fd": sock.fileno(),
            "released": False,
        },
    )
    # 安全兜底，避免信号丢失时永久残留
    deadline = time.time() + 300
    while time.time() < deadline:
        time.sleep(0.2)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["shared", "exiting"], required=True)
    parser.add_argument("--closer-host", default="127.0.0.1")
    parser.add_argument("--closer-port", type=int, default=9000)
    parser.add_argument("--status-file", required=True)
    parser.add_argument("--children", type=int, default=2)
    parser.add_argument("--duration", type=float, default=120.0)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.mode == "shared":
        sys.exit(run_shared(args))
    sys.exit(run_exiting(args))

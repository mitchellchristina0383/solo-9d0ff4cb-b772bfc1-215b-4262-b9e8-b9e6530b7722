"""closer 端：接受连接后立刻关闭，对端 holder 因此进入 CLOSE_WAIT。

作为 sample 容器内的后台线程运行，监听 127.0.0.1:9000。
"""

from __future__ import annotations

import socketserver
import threading


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            self.request.recv(64)  # 等 holder 发出第一字节
        except OSError:
            pass
        # handler 返回即关闭连接，holder 收到 FIN 且自身不 close -> CLOSE_WAIT


class ThreadingCloser(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def start_closer(host: str = "127.0.0.1", port: int = 9000) -> ThreadingCloser:
    server = ThreadingCloser((host, port), _Handler)
    thread = threading.Thread(target=server.serve_forever, name="closer", daemon=True)
    thread.start()
    return server

# CLOSE_WAIT 连接巡检服务

值班场景：TCP 连接迟迟不释放，需要确认**哪些进程还持有处于 CLOSE_WAIT 的套接字**。
本服务提供 FastAPI 巡检接口，输入一组 PID 和采样间隔，从 Linux `/proc` 采集两轮
IPv4 TCP 状态与套接字归属，输出仍被持有的 CLOSE_WAIT 连接及对应 PID/FD。

## 工作原理

每个 PID 每轮采样读取三类 procfs 信息（全程只读，不发信号、不操作 FD，对业务零干扰）：

| 路径 | 用途 |
| --- | --- |
| `/proc/<pid>/stat` | 取 `starttime`（第 22 字段）+ `comm`，用于识别 PID 换代 |
| `/proc/<pid>/net/tcp` | 该进程网络命名空间下的 IPv4 TCP 表（连接四元组、状态码、socket inode） |
| `/proc/<pid>/fd/*` | 符号链接 `socket:[inode]`，建立 socket inode → FD 列表的归属 |

判定规则：

1. **两轮均为 CLOSE_WAIT 的同一连接才输出，且只列一次**（按四元组匹配，比对两轮 inode）。
2. 持有者只列**第二轮确认仍存活且未换代**的受查 PID，并给出其持有的 FD。
3. PID 换代（同 PID 但 `starttime` 变化）或进程中途退出 → 进入 `unconfirmable_pids`，
   其余 PID 照常采样、照常出报告。
4. 巡检在线程池中执行，不阻塞事件循环，**巡检期间服务照常接入**（见并发测试）。

## 目录结构

```
app/                 巡检 API（FastAPI）
  inspector.py       /proc 解析与两轮采样核心逻辑（可注入 proc_root 便于测试）
  main.py            HTTP 端点 + 演示场景编排
sample/              本地样例：可复现 CLOSE_WAIT 的场景服务
  sample_app.py      场景控制（拉起/释放 holder 进程）
  holder.py          持有连接的样例进程（fork 共享 / 等待信号退出）
  closer.py          接受连接后立即关闭，使对端进入 CLOSE_WAIT
tests/               24 项测试：假 procfs 单测、API 测试、真实 /proc、真实服务 e2e
docker-compose.yml   两容器编排（api 共享 sample 的 PID 命名空间）
```

## 用 Docker Compose 启动

```bash
docker compose up --build
```

- 巡检 API：http://localhost:8000 ，交互式文档：http://localhost:8000/docs
- 样例场景服务：http://localhost:8001

> 关键配置：compose 中 `api` 服务使用 `pid: service:sample` 与 sample 共享 PID
> 命名空间，否则跨容器看不到 holder 进程的 `/proc/<pid>`。

## 在 /docs 试跑两个场景

### 场景一：共享连接进入 CLOSE_WAIT

`POST /demo/run-shared-close-wait`，请求体：

```json
{ "interval_seconds": 1, "children": 2 }
```

holder 父进程建连后 fork 出 2 个子进程，父子共 3 个 PID 持有**同一个 socket
（同一 inode）**；closer 主动断开后该连接在所有 holder 上进入 CLOSE_WAIT。
返回中该连接只出现一次，`holders_round2` 列出全部 3 个 PID 及各自 FD。

### 场景二：巡检中进程退出

`POST /demo/run-exit-during-inspection`，请求体：

```json
{ "interval_seconds": 2 }
```

服务在两轮采样间隔过半时通知 holder 退出；第二轮采样发现进程消失，该 PID 进入
`unconfirmable_pids`（含原因与第一轮身份信息），`rounds[1].failed_pids` 同步记录。

### 直接巡检任意 PID

`POST /api/v1/inspect`：

```json
{ "pids": [1234, 5678], "interval_seconds": 2 }
```

返回结构：

```json
{
  "pids": [1234, 5678],
  "interval_seconds": 2.0,
  "rounds": [ {"round": 1, "sampled_pids": [...], "failed_pids": [...], "...": "..." } ],
  "close_wait_connections": [
    {
      "connection": {"local": "10.0.0.2:40000", "remote": "10.0.0.1:9000",
                     "state": "CLOSE_WAIT", "inode": 89665175},
      "round1_inode": 89665175,
      "round2_inode": 89665175,
      "inode_stable": true,
      "holders_round2": [ {"pid": 1234, "comm": "holder", "fds": [3]} ]
    }
  ],
  "unconfirmable_pids": [
    {"pid": 5678, "reason": "process exited between the two rounds",
     "round1": {"pid": 5678, "comm": "python", "starttime": 1365, "error": null},
     "round2": null}
  ]
}
```

清理演示进程：`DELETE /demo/cleanup`。

## 不用 Docker 的本地运行方式

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
# 终端 1：样例场景服务
SAMPLE_STATE_DIR=/tmp/sample-state \
  python -m uvicorn sample_app:app --app-dir sample --port 8001
# 终端 2：巡检 API（同机即同 PID 命名空间，无需 pid 共享）
SAMPLE_BASE_URL=http://127.0.0.1:8001 \
  python -m uvicorn app.main:app --port 8000
```

## 测试

```bash
pip install -r requirements.txt pytest
pytest                      # 全部 24 项（含真实进程 e2e，约 10s）
pytest -m "not live"        # 跳过需要拉起 uvicorn 的 e2e
```

## 边界与限制

- 仅采集 **IPv4**（`/proc/<pid>/net/tcp`）；IPv6 的 `tcp6` 不在范围内。
- 连接身份按四元组匹配，并额外比对两轮 socket inode（`inode_stable`）。
- 受查 PID 不属于当前用户且无权读其 `/proc/<pid>/fd` 时，该 PID 计入
  `unconfirmable_pids`，不影响其他 PID；容器内默认以 root 运行可规避。
- 采样间隔内恰好完成关闭并被新连接复用四元组的极端情况，靠两轮 inode 比对兜底识别。

# FastAPI 流式 API 演示

一个可运行的 FastAPI 流式接口示例，包含四种常见形式：纯文本流、SSE、NDJSON 和模拟大模型聊天流。附带浏览器演示页与命令行客户端。

## 目录结构

```
fastapi_streaming_demo/
├── main.py            # FastAPI 应用与全部流式端点
├── client.py          # 命令行流式客户端（httpx）
├── static/
│   └── index.html     # 浏览器演示页面
├── requirements.txt
└── README.md
```

## 快速开始

```bash
cd fastapi_streaming_demo

# 建议使用虚拟环境
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux

pip install -r requirements.txt
uvicorn main:app --reload
```

启动后访问：

- 浏览器演示页：<http://127.0.0.1:8000/>
- 自动生成的接口文档：<http://127.0.0.1:8000/docs>

也可以直接运行命令行客户端（需要服务已启动）：

```bash
python client.py
```

## 端点一览

| 方法 | 路径 | Content-Type | 说明 |
| --- | --- | --- | --- |
| GET | `/api/stream/plain` | `text/plain` | 逐片段输出一段文字 |
| GET | `/api/stream/sse` | `text/event-stream` | 推送 `progress` / `done` 事件 |
| GET | `/api/stream/ndjson` | `application/x-ndjson` | 每行一条 JSON 记录 |
| POST | `/api/chat/stream` | `text/event-stream` | 模拟 LLM 逐 token 回复 |

### curl 验证

```bash
curl -N http://127.0.0.1:8000/api/stream/plain
curl -N http://127.0.0.1:8000/api/stream/sse
curl -N "http://127.0.0.1:8000/api/stream/ndjson?limit=5"

curl -N -X POST http://127.0.0.1:8000/api/chat/stream \
  -H "Content-Type: application/json" \
  -d '{"message":"你好","speed":0.05}'
```

`-N` 用于关闭 curl 自身的输出缓冲。Windows PowerShell 中请使用 `curl.exe`，JSON 参数按 PowerShell 规则转义。

SSE 响应示例：

```
event: progress
data: {"step": 1, "total": 10, "percent": 10, "ts": 1763000000.0}

event: done
data: {"message": "任务完成", "ts": 1763000004.0}
```

## 核心实现要点

1. **异步生成器 + `StreamingResponse`**：生成器每 `yield` 一次，Starlette 就把该分片写入响应，不会等全部内容生成完。
2. **选对媒体类型**：纯文本用 `text/plain`，事件流用 `text/event-stream`，逐行 JSON 用 `application/x-ndjson`。
3. **SSE 格式**：每条事件由 `event:`、`data:` 行组成，并以一个空行结束；`data` 必须是单行文本，复杂结构要先 `json.dumps`。
4. **禁用缓冲**：演示中设置了 `Cache-Control: no-cache` 和 `X-Accel-Buffering: no`。生产环境若经过 Nginx，需配置 `proxy_buffering off;`；同时避免对 `text/event-stream` 启用 gzip 压缩中间件。
5. **处理断开**：生成器里用 `await request.is_disconnected()` 提前结束；客户端中断时 Starlette 也可能取消生成器，因此捕获 `asyncio.CancelledError` 后重新抛出。
6. **POST + SSE**：浏览器原生 `EventSource` 只支持 GET，聊天示例改用 `fetch` 读取 `response.body`，再手动按 `\n\n` 分帧解析。
7. **不要阻塞事件循环**：示例使用 `async def` 生成器与 `asyncio.sleep`。若调用同步的模型 SDK，请放入线程池（如 `anyio.to_thread.run_sync`）或改用其异步接口。

## 接入真实模型

`main.py` 中的 `CHAT_REPLY` 只是静态模拟文本。接入真实服务时，把 `chat_stream` 生成器里的循环替换为模型 SDK 的流式迭代即可，例如：

```python
async for chunk in model.stream(payload.message):
    if await request.is_disconnected():
        return
    yield sse_event({"text": chunk}, event="token")
```

## 停止服务

命令行窗口按 `Ctrl+C`。

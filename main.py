"""FastAPI 流式 API 演示。

演示四种常见的流式响应形式：

1. ``GET  /api/stream/plain``   纯文本流（``StreamingResponse`` + 异步生成器）
2. ``GET  /api/stream/sse``     SSE / Server-Sent Events（结构化事件流）
3. ``GET  /api/stream/ndjson``  NDJSON（每行一条 JSON 记录）
4. ``POST /api/chat/stream``    模拟大模型聊天（POST + SSE，逐 token 推送）

启动方式::

    uvicorn main:app --reload

接口文档：http://127.0.0.1:8000/docs
浏览器演示：http://127.0.0.1:8000/
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("streaming-demo")

app = FastAPI(
    title="FastAPI 流式 API 演示",
    version="1.0.0",
    description="演示纯文本流、SSE、NDJSON 和模拟 LLM 聊天流。",
)

# 流式响应通用响应头：
# - Cache-Control: no-cache 避免浏览器/代理缓存分片响应
# - X-Accel-Buffering: no 关闭 Nginx 的响应缓冲，确保数据逐条到达
STREAM_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}

PLAIN_TEXT = (
    "这是 FastAPI 的纯文本流式响应示例。"
    "服务端使用 async 生成器逐段产出内容，StreamingResponse 会立即把每一段发送给客户端，"
    "而不必等所有内容生成完毕。你看到这段文字时，它正一个片段一个片段地到达。"
)

CHAT_REPLY = (
    "这是一个模拟的大语言模型流式回复：真实的模型在生成每个 token 时就会立即推送，"
    "而不是等整段文字生成完再一次性返回。FastAPI 的 StreamingResponse 配合异步生成器，"
    "可以把结果低延迟地一点点送到客户端。流结束时会发送 done 事件，其中包含耗时和 token 数量。"
)


def chunk_text(text: str, size: int = 2) -> list[str]:
    """把文本切成固定长度的小片段，用来模拟 token 流。"""
    return [text[i : i + size] for i in range(0, len(text), size)]


def sse_event(data: Any, event: str | None = None) -> str:
    """按 SSE 规范格式化一条事件（每条事件以空行结尾）。"""
    lines = []
    if event:
        lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(data, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    """返回浏览器演示页面。"""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/stream/plain", summary="纯文本流")
async def stream_plain(
    request: Request,
    delay: float = Query(0.05, ge=0, le=2, description="每个片段之间的延迟（秒）"),
) -> StreamingResponse:
    """以 ``text/plain`` 的形式逐片段返回一段文字。"""

    async def generate() -> AsyncIterator[str]:
        try:
            for piece in chunk_text(PLAIN_TEXT, 2):
                if await request.is_disconnected():
                    logger.info("客户端已断开，停止纯文本流")
                    return
                yield piece
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logger.info("纯文本流被取消")
            raise

    return StreamingResponse(
        generate(),
        media_type="text/plain; charset=utf-8",
        headers=STREAM_HEADERS,
    )


@app.get("/api/stream/sse", summary="SSE 进度流")
async def stream_sse(request: Request) -> StreamingResponse:
    """用 Server-Sent Events 推送带事件名和 JSON 数据的进度信息。"""

    async def generate() -> AsyncIterator[str]:
        total = 10
        try:
            for step in range(1, total + 1):
                if await request.is_disconnected():
                    logger.info("客户端已断开，停止 SSE 流")
                    return
                yield sse_event(
                    {
                        "step": step,
                        "total": total,
                        "percent": round(step / total * 100),
                        "ts": time.time(),
                    },
                    event="progress",
                )
                await asyncio.sleep(0.4)
            yield sse_event(
                {"message": "任务完成", "ts": time.time()},
                event="done",
            )
        except asyncio.CancelledError:
            logger.info("SSE 流被取消")
            raise

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers=STREAM_HEADERS,
    )


@app.get("/api/stream/ndjson", summary="NDJSON 数据流")
async def stream_ndjson(
    request: Request,
    limit: int = Query(20, ge=1, le=100, description="要生成的记录条数"),
) -> StreamingResponse:
    """以 ``application/x-ndjson`` 连续输出 JSON 记录，每行一条。"""

    async def generate() -> AsyncIterator[str]:
        try:
            for i in range(1, limit + 1):
                if await request.is_disconnected():
                    logger.info("客户端已断开，停止 NDJSON 流")
                    return
                record = {
                    "id": i,
                    "value": round(random.uniform(0, 100), 2),
                    "ts": time.time(),
                }
                yield json.dumps(record, ensure_ascii=False) + "\n"
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            logger.info("NDJSON 流被取消")
            raise

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers=STREAM_HEADERS,
    )


class ChatRequest(BaseModel):
    """聊天请求体。"""

    message: str = Field(..., min_length=1, max_length=2000, description="用户消息")
    speed: float = Field(
        0.06, ge=0, le=1, description="每个 token 之间的延迟（秒）"
    )


@app.post("/api/chat/stream", summary="模拟 LLM 聊天流")
async def chat_stream(payload: ChatRequest, request: Request) -> StreamingResponse:
    """接收 POST 消息，并以 SSE 逐 token 返回一条模拟回复。

    ``EventSource`` 只支持 GET，因此浏览器端使用 ``fetch`` + ``ReadableStream``
    读取并解析这个响应。
    """
    request_id = uuid.uuid4().hex[:8]
    started = time.perf_counter()

    async def generate() -> AsyncIterator[str]:
        tokens = chunk_text(CHAT_REPLY, 2)
        sent = 0
        try:
            yield sse_event(
                {
                    "request_id": request_id,
                    "model": "demo-model",
                    "received": payload.message,
                },
                event="start",
            )
            for token in tokens:
                if await request.is_disconnected():
                    logger.info("客户端已断开，停止聊天流 request_id=%s", request_id)
                    return
                yield sse_event({"text": token}, event="token")
                sent += 1
                await asyncio.sleep(payload.speed)
            yield sse_event(
                {
                    "request_id": request_id,
                    "tokens": sent,
                    "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
                },
                event="done",
            )
        except asyncio.CancelledError:
            logger.info("聊天流被取消 request_id=%s", request_id)
            raise
        finally:
            logger.info(
                "聊天流结束 request_id=%s tokens=%d elapsed=%.0fms",
                request_id,
                sent,
                (time.perf_counter() - started) * 1000,
            )

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers=STREAM_HEADERS,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

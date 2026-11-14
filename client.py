"""命令行流式客户端，依次消费演示服务的四个流式端点。

先启动服务::

    uvicorn main:app --reload

再运行::

    python client.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

import httpx

# 重定向/管道输出时避免中文乱码
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

BASE_URL = "http://127.0.0.1:8000"


def parse_sse_line(line: str, event: str, data_lines: list[str]) -> tuple[str, list[str]]:
    """收集一帧 SSE 的 event 和 data 行。"""
    if line.startswith("event:"):
        event = line[len("event:"):].strip()
    elif line.startswith("data:"):
        data_lines.append(line[len("data:"):].strip())
    return event, data_lines


async def demo_plain(client: httpx.AsyncClient) -> None:
    print("\n=== 1. 纯文本流 /api/stream/plain ===")
    async with client.stream("GET", f"{BASE_URL}/api/stream/plain") as response:
        response.raise_for_status()
        async for chunk in response.aiter_text():
            print(chunk, end="", flush=True)
    print()


async def demo_sse(client: httpx.AsyncClient) -> None:
    print("\n=== 2. SSE 进度流 /api/stream/sse ===")
    async with client.stream("GET", f"{BASE_URL}/api/stream/sse") as response:
        response.raise_for_status()
        event = "message"
        data_lines: list[str] = []
        async for line in response.aiter_lines():
            if line == "":
                if data_lines:
                    payload = json.loads("\n".join(data_lines))
                    print(f"[{event}] {payload}")
                    if event == "done":
                        return
                event, data_lines = "message", []
                continue
            event, data_lines = parse_sse_line(line, event, data_lines)


async def demo_ndjson(client: httpx.AsyncClient) -> None:
    print("\n=== 3. NDJSON 数据流 /api/stream/ndjson?limit=5 ===")
    async with client.stream(
        "GET", f"{BASE_URL}/api/stream/ndjson", params={"limit": 5}
    ) as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if line.strip():
                print(json.loads(line))


async def demo_chat(client: httpx.AsyncClient) -> None:
    print("\n=== 4. 聊天流 /api/chat/stream（POST + SSE） ===")
    payload: dict[str, Any] = {"message": "你好，请介绍一下流式响应", "speed": 0.02}
    async with client.stream(
        "POST", f"{BASE_URL}/api/chat/stream", json=payload
    ) as response:
        response.raise_for_status()
        event = "message"
        data_lines: list[str] = []
        async for line in response.aiter_lines():
            if line == "":
                if data_lines:
                    data = json.loads("\n".join(data_lines))
                    if event == "start":
                        print(f"[start] request_id={data['request_id']} model={data['model']}")
                    elif event == "token":
                        print(data["text"], end="", flush=True)
                    elif event == "done":
                        print(
                            f"\n[done] tokens={data['tokens']} "
                            f"elapsed={data['elapsed_ms']}ms"
                        )
                        return
                event, data_lines = "message", []
                continue
            event, data_lines = parse_sse_line(line, event, data_lines)


async def main() -> None:
    async with httpx.AsyncClient(timeout=None) as client:
        try:
            await demo_plain(client)
            await demo_sse(client)
            await demo_ndjson(client)
            await demo_chat(client)
        except httpx.ConnectError:
            print(f"无法连接 {BASE_URL}，请先运行：uvicorn main:app --reload")


if __name__ == "__main__":
    asyncio.run(main())

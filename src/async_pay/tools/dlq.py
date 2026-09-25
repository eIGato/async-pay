"""Inspect the dead-letter queue without draining it.

``basic.get`` each message, print it, then nack it back onto the queue, so the
DLQ keeps its contents and stays a durable record of what failed. Run with::

    python -m async_pay.tools.dlq [limit]
"""

import asyncio
import json
import sys

import aio_pika

from async_pay.config import Settings, get_settings

DEFAULT_LIMIT = 20


def _format_body(body: bytes) -> str:
    try:
        return json.dumps(json.loads(body), ensure_ascii=False, sort_keys=True)
    except (ValueError, UnicodeDecodeError):
        return repr(body)


async def peek(settings: Settings, limit: int = DEFAULT_LIMIT) -> int:
    """Print up to ``limit`` dead-lettered messages, leaving them in place."""
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    seen = 0
    try:
        channel = await connection.channel()
        queue = await channel.get_queue(settings.payments_dlq, ensure=True)
        # Hold every message unacked until the end: nacking one at a time would
        # return it to the head of the queue and we would read it again.
        held = []
        while seen < limit:
            message = await queue.get(no_ack=False, fail=False)
            if message is None:
                break
            held.append(message)
            seen += 1
            headers = message.headers or {}
            death = headers.get("x-death")
            print(
                f"[{seen}] message_id={message.message_id} "
                f"deaths={death if death else 'n/a'} "
                f"body={_format_body(message.body)}"
            )
        for message in held:
            await message.nack(requeue=True)

        if seen == 0:
            print(f"{settings.payments_dlq} is empty")
        else:
            print(f"-- {seen} message(s) shown and returned to {settings.payments_dlq}")
        return seen
    finally:
        await connection.close()


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LIMIT
    asyncio.run(peek(get_settings(), limit))


if __name__ == "__main__":
    main()

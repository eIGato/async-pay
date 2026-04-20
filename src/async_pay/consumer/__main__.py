import asyncio

from async_pay.consumer.app import app

if __name__ == "__main__":
    asyncio.run(app.run())

"""Submit a greeting to a running local Ledgence instance."""
import asyncio

from ledgence.client import AsyncClient


async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        task = await client.tasks.submit(
            program="hello-ledgence",
            version="0.1.0",
            queue="demo",
            data={"name": "Ledgence"},  # This becomes event["data"] in program.py.
            idempotency_key="hello-task-1",  # Reuse this key to find the same task.
        )
        print("Task:", task.id)
        output = await task.result(timeout=60)  # Wait here for up to 60 seconds.
        print(output["message"])


if __name__ == "__main__":
    asyncio.run(main())

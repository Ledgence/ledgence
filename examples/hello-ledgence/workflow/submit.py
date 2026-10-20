"""Submit the workflow that calls the previously published greeting task."""
import asyncio

from ledgence.client import AsyncClient


async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        workflow = await client.workflows.submit(
            program="hello-workflow",
            version="0.1.0",
            queue="demo",
            data={"name": "Ledgence"},
            idempotency_key="hello-workflow-1",
        )
        print("Workflow:", workflow.id)
        output = await workflow.result(timeout=60)
        print(output["message"])


if __name__ == "__main__":
    asyncio.run(main())

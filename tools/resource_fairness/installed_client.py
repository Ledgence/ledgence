"""Executed only via the wheel-installed client interpreter in isolated mode (MIT)."""

import asyncio
import json
import sys
from dataclasses import asdict
from pathlib import Path

import ledgence.client
from ledgence.client import AsyncClient, ResourcePolicy


async def check():
    server, credential, tenant, namespace, name, samples = sys.argv[1:]
    assert (
        Path(ledgence.client.__file__)
        .resolve()
        .is_relative_to(Path(sys.prefix).resolve())
    )
    async with AsyncClient(
        server, tenant=tenant, namespace=namespace, credentials_file=credential
    ) as client:
        before = None
        for expected in json.loads(Path(samples).read_text()):
            page = await client.resources.decisions(name, before=before, limit=2)
            wire = []
            for decision in page.items:
                item = asdict(decision)
                for field in ("sequence", "offer_generation", "policy_revision"):
                    item[field] = str(item[field])
                wire.append(item)
            assert wire == expected["items"]
            before = page.next
            assert (None if before is None else str(before)) == expected["next"]
        previous = (await client.resources.get("surface-policy")).resource
        configured = previous.policy.fair_sharing
        assert previous.revision == 2 and configured.default_group == "batch"
        assert [(g.name, g.weight, g.queues) for g in configured.groups] == [
            ("support", 3, ("support",)),
            ("batch", 1, ()),
        ]
        policy = ResourcePolicy(5, fair_sharing=configured)
        command = {
            "policy": policy,
            "expected_revision": 2,
            "request_id": "python-surface",
            "reason": "wheel qualification",
        }
        receipt = await client.resources.put("surface-policy", **command)
        replay = await client.resources.put("surface-policy", **command)
        assert receipt.resource == replay.resource and replay.already_accepted
        assert receipt.resource.revision == 3 and receipt.resource.policy == policy
    print(json.dumps({"read_pages": 2, "policy_revision": receipt.resource.revision}))


if __name__ == "__main__":
    asyncio.run(check())

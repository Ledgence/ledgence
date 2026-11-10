"""Read a report on another worker without shared local paths (MIT)."""
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

from ledgence.worker import artifacts


async def handle(event):
    reference = event["data"]["artifact"]
    with TemporaryDirectory() as directory:
        path = await artifacts.download(reference, Path(directory) / "report.txt")
        # Download already verifies the complete digest before publishing path.
        data = path.read_bytes()
    return {"bytes": len(data), "digest": "sha256:" + hashlib.sha256(data).hexdigest()}

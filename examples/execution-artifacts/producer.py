"""Create a report and return a small durable reference instead of file bytes (MIT)."""
from pathlib import Path
from tempfile import TemporaryDirectory

from ledgence.worker import artifacts


async def handle(event):
    size = event["data"].get("size", 300_000)
    if type(size) is not int or not 0 <= size <= 1_000_000:
        raise ValueError("size must be an integer between 0 and 1,000,000 bytes")
    # A real agent could write its report or generated patch here instead.
    with TemporaryDirectory() as directory:
        report = Path(directory) / "report.txt"
        report.write_bytes(b"x" * size)
        reference = await artifacts.put_file(report, key="report", media_type="text/plain")
    return {"artifact": reference}

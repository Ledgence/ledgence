"""Bounded async subprocess IO, retaining the worker's process group (MIT)."""
import asyncio
from contextlib import suppress


class ProcessError(RuntimeError):
    pass


async def collect(argv, *, cwd, environment, data=b"", timeout=120, stdout_limit=128 * 1024,
                  stderr_limit=32 * 1024):
    process = None
    tasks = []
    cleanup_failed = False
    async def read(stream, maximum):
        output = bytearray()
        while chunk := await stream.read(8192):
            output.extend(chunk)
            if len(output) > maximum:
                raise ProcessError("subprocess output limit exceeded")
        return bytes(output)

    async def write():
        try:
            process.stdin.write(data)
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            process.stdin.close()

    try:
        async with asyncio.timeout(timeout):
            process = await asyncio.create_subprocess_exec(
                *argv, cwd=cwd, env=environment, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=False)
            tasks = [asyncio.create_task(read(process.stdout, stdout_limit)),
                     asyncio.create_task(read(process.stderr, stderr_limit)),
                     asyncio.create_task(write()), asyncio.create_task(process.wait())]
            stdout, stderr, _, code = await asyncio.gather(*tasks)
            return code, stdout, stderr
    except TimeoutError:
        raise ProcessError("subprocess execution deadline exceeded") from None
    except OSError:
        raise ProcessError("could not start or communicate with subprocess") from None
    finally:
        if process is not None:
            if process.returncode is None:
                with suppress(ProcessLookupError):
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 2)
                except TimeoutError:
                    with suppress(ProcessLookupError):
                        process.kill()
                    try:
                        await asyncio.wait_for(process.wait(), 2)
                    except TimeoutError:
                        cleanup_failed = True
            process.stdin.close()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if cleanup_failed:
            raise ProcessError("subprocess cleanup deadline exceeded") from None

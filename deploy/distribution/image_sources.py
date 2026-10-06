#!/usr/bin/env python3
"""Collect exact Debian/CPython runtime sources for an image distribution artifact.

Only Python's standard library is used. Sources are shipped beside the OCI image,
not inside its runtime layers. A missing source or checksum fails the collection.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

SNAPSHOT = "https://snapshot.debian.org"
SHA256 = re.compile(r"[a-f0-9]{64}")
PACKAGE = re.compile(r"[a-z0-9][a-z0-9+.-]+")
VERSION = re.compile(r"[A-Za-z0-9.+:~\-]+")
FILENAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+~\-]*")


def stream_digest(stream):
    checksum = hashlib.sha256()
    while block := stream.read(1024 * 1024):
        checksum.update(block)
    return checksum.hexdigest()


def digest(path):
    with path.open("rb") as stream:
        return stream_digest(stream)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def https_url(url):
    parsed = urlparse(url)
    require(parsed.scheme == "https" and parsed.hostname and not parsed.username
            and not parsed.password and not parsed.fragment, "source URL must use HTTPS without credentials")
    return url


class Downloader:
    def __init__(self, attempts=4):
        self.attempts = attempts

    def fetch(self, url, destination, *, sha256=None, sha1=None, size=None, max_size=2 * 1024**3):
        https_url(url)
        destination.parent.mkdir(parents=True, exist_ok=True)
        require(not destination.exists(), f"download destination already exists: {destination}")
        temporary = destination.with_name(destination.name + ".partial")
        for attempt in range(self.attempts):
            try:
                request = Request(url, headers={"User-Agent": "Ledgence-source-bundle/1"})
                with urlopen(request, timeout=60) as response, temporary.open("wb") as stream:
                    https_url(response.url)
                    actual_size = 0
                    actual_sha256 = hashlib.sha256()
                    actual_sha1 = hashlib.sha1()
                    while block := response.read(1024 * 1024):
                        actual_size += len(block)
                        require(actual_size <= (size if size is not None else max_size),
                                f"source exceeds expected size: {url}")
                        actual_sha256.update(block)
                        actual_sha1.update(block)
                        stream.write(block)
                require(size is None or actual_size == size, f"source size mismatch: {url}")
                require(sha256 is None or actual_sha256.hexdigest() == sha256,
                        f"source SHA-256 mismatch: {url}")
                require(sha1 is None or actual_sha1.hexdigest() == sha1,
                        f"snapshot content identity mismatch: {url}")
                temporary.rename(destination)
                return
            except (HTTPError, URLError, TimeoutError, ConnectionError) as error:
                if isinstance(error, HTTPError) and error.code not in (408, 429, 500, 502, 503, 504):
                    raise
                if attempt + 1 == self.attempts:
                    raise
                time.sleep(min(2**attempt, 8))
            finally:
                temporary.unlink(missing_ok=True)

    def json(self, url):
        with tempfile.TemporaryDirectory(prefix="ledgence-source-index-") as temporary:
            path = Path(temporary) / "index.json"
            self.fetch(url, path, max_size=8 * 1024**2)
            return json.loads(path.read_text())


def validate_inventory(value):
    require(isinstance(value, dict) and value.get("format") == 1, "unsupported runtime inventory")
    require(re.fullmatch(r"[^\s]+@sha256:[a-f0-9]{64}", value.get("base_image", "")),
            "runtime inventory must identify a digest-pinned base image")
    python = value.get("python", {})
    version = python.get("version", "")
    require(re.fullmatch(r"3\.[0-9]+\.[0-9]+", version), "invalid CPython version")
    require(python.get("source_url") ==
            f"https://www.python.org/ftp/python/{version}/Python-{version}.tar.xz",
            "CPython source URL must match its exact version")
    require(SHA256.fullmatch(python.get("sha256", "")), "missing CPython source SHA-256")
    recipe = value.get("base_recipe", {})
    require(re.fullmatch(
        r"https://raw\.githubusercontent\.com/docker-library/python/[a-f0-9]{40}/[A-Za-z0-9./_-]+/Dockerfile",
        recipe.get("url", "")), "base recipe must identify an immutable docker-library/python commit")
    require(SHA256.fullmatch(recipe.get("sha256", "")), "missing base recipe SHA-256")
    packages = value.get("packages")
    require(isinstance(packages, list) and packages, "runtime package inventory is empty")
    seen = set()
    for package in packages:
        require(isinstance(package, dict), "invalid runtime package record")
        require(PACKAGE.fullmatch(package.get("source_name", "")), "invalid package source_name")
        binary = package.get("binary_name", "").split(":")
        require(len(binary) <= 2 and PACKAGE.fullmatch(binary[0]) and
                (len(binary) == 1 or binary[1] == package.get("architecture")),
                "invalid or mismatched binary package qualifier")
        for key in ("binary_version", "source_version"):
            require(VERSION.fullmatch(package.get(key, "")), f"invalid package {key}")
        require(re.fullmatch(r"[a-z0-9-]+", package.get("architecture", "")), "invalid package architecture")
        identity = binary[0], package["architecture"]
        require(identity not in seen, "duplicate binary package identity")
        seen.add(identity)
    return value


def parse_dsc(content, package, version):
    # A .dsc is a Debian control record, optionally wrapped in a cleartext PGP
    # signature. Its payload SHA-256 checksums are verified below. This collector
    # does not claim to verify the maintainer's PGP signature.
    text = content.decode("utf-8")
    if text.startswith("-----BEGIN PGP SIGNED MESSAGE-----"):
        require("\n\n" in text, "invalid signed source control record")
        text = text.split("\n\n", 1)[1].split("-----BEGIN PGP SIGNATURE-----", 1)[0]
        text = "\n".join(line[2:] if line.startswith("- ") else line for line in text.splitlines())
    fields = {}
    current = None
    for line in text.splitlines():
        if not line:
            continue
        if line[0].isspace():
            require(current is not None, "invalid source control continuation")
            fields[current] += "\n" + line.strip()
        else:
            require(":" in line, "invalid source control field")
            current, value = line.split(":", 1)
            require(current not in fields, "duplicate source control field")
            fields[current] = value.strip()
    require(fields.get("Source") == package and fields.get("Version") == version,
            "source control package/version differs from runtime inventory")
    payloads = {}
    for line in fields.get("Checksums-Sha256", "").splitlines():
        if not line:
            continue
        parts = line.split()
        require(len(parts) == 3 and SHA256.fullmatch(parts[0]) and parts[1].isdigit(),
                "invalid source SHA-256 record")
        checksum, size, name = parts
        require(FILENAME.fullmatch(name) and name not in payloads, "invalid or duplicate source filename")
        payloads[name] = {"sha256": checksum, "size": int(size)}
    require(payloads, "source control record has no SHA-256 payload inventory")
    return payloads


def file_record(path, root, url, **extra):
    return {"path": str(path.relative_to(root)), "sha256": digest(path),
            "size": path.stat().st_size, "url": url, **extra}


def collect_debian(package, version, root, downloader):
    endpoint = f"{SNAPSHOT}/mr/package/{quote(package, safe='')}/{quote(version, safe='')}/srcfiles?fileinfo=1"
    index = downloader.json(endpoint)
    require(index.get("package") == package and index.get("version") == version,
            "snapshot index differs from requested source identity")
    files = {}
    hashes = set()
    for item in index.get("result", []):
        checksum = item.get("hash", "")
        require(re.fullmatch(r"[a-f0-9]{40}", checksum), "invalid snapshot content identity")
        hashes.add(checksum)
        info = index.get("fileinfo", {}).get(checksum, [])
        require(info, "snapshot index has no filename information")
        for entry in info:
            name, size = entry.get("name", ""), entry.get("size")
            require(FILENAME.fullmatch(name) and type(size) is int and size >= 0,
                    "invalid snapshot file metadata")
            record = {"sha1": checksum, "size": size}
            require(name not in files or files[name] == record, "ambiguous snapshot filename")
            files[name] = record
    dsc_names = [name for name in files if name.endswith(".dsc")]
    require(len(dsc_names) == 1, "expected exactly one source control record")
    directory = root / "debian" / package / quote(version, safe="")
    dsc_name = dsc_names[0]
    dsc = directory / dsc_name
    dsc_info = files[dsc_name]
    url = f"{SNAPSHOT}/file/{dsc_info['sha1']}"
    downloader.fetch(url, dsc, **dsc_info)
    records = [file_record(dsc, root, url, snapshot_sha1=dsc_info["sha1"])]
    payloads = parse_dsc(dsc.read_bytes(), package, version)
    used_hashes = {dsc_info["sha1"]}
    for name, expected in sorted(payloads.items()):
        require(name in files and files[name]["size"] == expected["size"],
                "source control payload is missing from snapshot index")
        info = files[name]
        source_url = f"{SNAPSHOT}/file/{info['sha1']}"
        path = directory / name
        downloader.fetch(source_url, path, sha1=info["sha1"], **expected)
        used_hashes.add(info["sha1"])
        records.append(file_record(path, root, source_url, snapshot_sha1=info["sha1"]))
    require(used_hashes == hashes, "snapshot source inventory has unaccounted files")
    return {"name": package, "version": version, "index_url": endpoint, "files": records}


def archive_tree(root, archive):
    with archive.open("wb") as output, gzip.GzipFile(fileobj=output, mode="wb", mtime=0, filename="") as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as tar:
            for path in sorted(root.rglob("*")):
                if not path.is_file():
                    continue
                entry = tar.gettarinfo(str(path), arcname="corresponding-source/" + str(path.relative_to(root)))
                entry.uid = entry.gid = entry.mtime = 0
                entry.uname = entry.gname = ""
                entry.mode = 0o644
                with path.open("rb") as stream:
                    tar.addfile(entry, stream)


def collect(inventory_path, output, *, downloader=None, workers=4, image=None):
    require(1 <= workers <= 8, "workers must be between 1 and 8")
    require(not output.exists(), "output directory already exists; choose a new path")
    inventory_bytes = inventory_path.read_bytes()
    inventory = validate_inventory(json.loads(inventory_bytes))
    downloader = downloader or Downloader()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".ledgence-sources-", dir=output.parent) as temporary:
        stage = Path(temporary) / "artifact"
        root = stage / "sources"
        root.mkdir(parents=True)
        (root / "runtime-inventory.json").write_bytes(inventory_bytes)
        identities = sorted({(package["source_name"], package["source_version"])
                             for package in inventory["packages"]})
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = [executor.submit(collect_debian, *identity, root, downloader) for identity in identities]
            try:
                sources = [future.result() for future in pending]
            except BaseException:
                for future in pending:
                    future.cancel()
                raise
        python = inventory["python"]
        python_path = root / "python" / f"Python-{python['version']}.tar.xz"
        downloader.fetch(python["source_url"], python_path, sha256=python["sha256"])
        recipe = inventory["base_recipe"]
        recipe_path = root / "python" / "Dockerfile"
        downloader.fetch(recipe["url"], recipe_path, sha256=recipe["sha256"])
        recipe_text = recipe_path.read_text()
        require(f"ENV PYTHON_VERSION {python['version']}\n" in recipe_text and
                f"ENV PYTHON_SHA256 {python['sha256']}\n" in recipe_text,
                "base build recipe differs from the runtime Python source identity")
        manifest = {
            "format": 1, "complete": True, "scope": "Debian packages and CPython in the runtime image",
            "base_image": inventory["base_image"], "image": image,
            "runtime_inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
            "binary_package_count": len(inventory["packages"]), "source_package_count": len(sources),
            "debian": sources,
            "python": file_record(python_path, root, python["source_url"]),
            "base_recipe": file_record(recipe_path, root, recipe["url"]),
        }
        manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
        (root / "source-manifest.json").write_bytes(manifest_bytes)
        (root / "README.txt").write_text(
            "Exact sources for the Debian and CPython components of this Ledgence runtime image.\n"
            "Third-party components retain their own licenses. This is not a relicensing or legal approval claim.\n"
            "runtime-inventory.json maps installed binaries to source package versions and retained notices.\n"
            "Each Debian directory contains its .dsc plus all SHA-256-verified source payloads; rebuild using\n"
            "Debian's dpkg-source/dpkg-buildpackage tooling and that package's build instructions.\n"
            "python/ contains the exact CPython source archive and immutable upstream image Dockerfile.\n"
            "Ledgence, Rust dependencies and Console notices/source identities are documented separately\n"
            "in the release and image legal inventories. No byte-identical rebuild claim is made.\n"
        )
        archive = stage / "corresponding-source.tar.gz"
        archive_tree(root, archive)
        (stage / "source-manifest.json").write_bytes(manifest_bytes)
        (stage / "SHA256SUMS").write_text(
            f"{digest(archive)}  corresponding-source.tar.gz\n"
            f"{digest(stage / 'source-manifest.json')}  source-manifest.json\n")
        shutil.rmtree(root)
        verify_output(stage, image=image["id"] if image else None,
                      platform=image["platform"] if image else None)
        require(not output.exists(), "output appeared during collection; refusing replacement")
        stage.rename(output)
    return manifest


def verify_output(directory, image=None, platform=None):
    """Verify the complete distributable without extracting or executing it."""
    names = {"corresponding-source.tar.gz", "source-manifest.json"}
    artifacts = list(directory.iterdir())
    require({path.name for path in artifacts} == names | {"SHA256SUMS"} and
            all(path.is_file() and not path.is_symlink() for path in artifacts),
            "unexpected corresponding-source artifact inventory")
    checksums = {}
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        parts = line.split()
        require(len(parts) == 2 and SHA256.fullmatch(parts[0]) and
                parts[1] in names and parts[1] not in checksums, "invalid source artifact checksums")
        checksums[parts[1]] = parts[0]
    require(set(checksums) == names, "incomplete source artifact checksums")
    for name, expected in checksums.items():
        require(digest(directory / name) == expected, "source artifact checksum mismatch")
    manifest_bytes = (directory / "source-manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    require(manifest.get("format") == 1 and manifest.get("complete") is True,
            "source collection is not complete")
    identity = manifest.get("image") or {}
    if image is not None:
        require(image == identity.get("id") or image == identity.get("requested_reference") or
                image in identity.get("repository_digests", []), "source artifact image identity mismatch")
    if platform is not None:
        require(platform == identity.get("platform"), "source artifact platform mismatch")
    records = {}
    for record in [manifest["python"], manifest["base_recipe"]] + [
            record for source in manifest["debian"] for record in source["files"]]:
        name = record["path"]
        path = PurePosixPath(name)
        require(not path.is_absolute() and ".." not in path.parts and str(path) == name and
                name not in records and SHA256.fullmatch(record["sha256"]) and
                type(record["size"]) is int and record["size"] >= 0, "invalid source manifest file record")
        records[name] = record
    expected_names = set(records) | {"runtime-inventory.json", "source-manifest.json", "README.txt"}
    seen = set()
    metadata = {}
    with tarfile.open(directory / "corresponding-source.tar.gz", "r:gz") as archive:
        for member in archive:
            require(member.isfile() and member.name.startswith("corresponding-source/"),
                    "source archive contains an unsupported member")
            name = member.name.removeprefix("corresponding-source/")
            require(name in expected_names and name not in seen, "source archive inventory mismatch")
            seen.add(name)
            with archive.extractfile(member) as stream:
                if name in records:
                    expected = records[name]
                    require(member.size == expected["size"] and
                            stream_digest(stream) == expected["sha256"],
                            "source archive payload checksum mismatch")
                if name in ("runtime-inventory.json", "source-manifest.json") or name.endswith(".dsc"):
                    require(member.size < 8 * 1024**2, "source archive metadata too large")
                    stream.seek(0)
                    metadata[name] = stream.read()
    require(seen == expected_names, "source archive is incomplete")
    require(metadata["source-manifest.json"] == manifest_bytes, "inner and outer source manifests differ")
    inventory_bytes = metadata["runtime-inventory.json"]
    require(hashlib.sha256(inventory_bytes).hexdigest() == manifest["runtime_inventory_sha256"],
            "source runtime inventory checksum mismatch")
    inventory = validate_inventory(json.loads(inventory_bytes))
    require(inventory["base_image"] == manifest["base_image"] and
            len(inventory["packages"]) == manifest["binary_package_count"], "source runtime inventory mismatch")
    source_identities = {(entry["source_name"], entry["source_version"]) for entry in inventory["packages"]}
    manifest_identities = {(entry["name"], entry["version"]) for entry in manifest["debian"]}
    require(source_identities == manifest_identities and
            len(manifest["debian"]) == manifest["source_package_count"] == len(source_identities),
            "source package coverage mismatch")
    require(manifest["python"]["sha256"] == inventory["python"]["sha256"] and
            manifest["python"]["url"] == inventory["python"]["source_url"] and
            manifest["base_recipe"]["sha256"] == inventory["base_recipe"]["sha256"] and
            manifest["base_recipe"]["url"] == inventory["base_recipe"]["url"], "Python source identity mismatch")
    for source in manifest["debian"]:
        dsc = [record for record in source["files"] if record["path"].endswith(".dsc")]
        require(len(dsc) == 1, "source package has no unique control record")
        payloads = parse_dsc(metadata[dsc[0]["path"]], source["name"], source["version"])
        actual = {PurePosixPath(record["path"]).name: {
            "sha256": record["sha256"], "size": record["size"]}
            for record in source["files"] if record is not dsc[0]}
        require(actual == payloads, "source package control/payload inventory mismatch")
    return manifest


def inventory_from_image(reference, platform, destination):
    result = subprocess.run(["docker", "image", "inspect", reference], check=True,
                            capture_output=True, text=True, timeout=60)
    inspected = json.loads(result.stdout)
    require(len(inspected) == 1, "expected one local image")
    image = inspected[0]
    image_id = image.get("Id", "")
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", image_id), "image has no immutable local identity")
    actual_platform = image.get("Os", "") + "/" + image.get("Architecture", "")
    require(actual_platform in ("linux/amd64", "linux/arm64"), "unsupported image platform")
    require(platform is None or platform == actual_platform, "image platform differs from requested platform")
    command = ["docker", "run", "--rm", "--network", "none", "--read-only", "--platform", actual_platform,
               "--entrypoint", "cat", image_id, "/opt/ledgence/legal/image-runtime.json"]
    result = subprocess.run(command, check=True, capture_output=True, timeout=60)
    destination.write_bytes(result.stdout)
    return {"requested_reference": reference, "id": image_id, "platform": actual_platform,
            "repository_digests": image.get("RepoDigests") or []}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--inventory", type=Path)
    source.add_argument("--image", help="Already loaded local image reference or immutable digest.")
    source.add_argument("--verify", type=Path, help="Verify a previously collected artifact directory.")
    parser.add_argument("--expected-image", help="Expected immutable image identity for --verify.")
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.verify:
        require(args.output is None, "--output cannot accompany --verify")
        manifest = verify_output(args.verify, image=args.expected_image, platform=args.platform)
        print(json.dumps({"verified": True, "source_packages": manifest["source_package_count"]}))
        return
    require(args.output is not None, "--output is required for collection")
    require(args.expected_image is None, "--expected-image requires --verify")
    require(args.image or args.platform is None, "--platform requires --image")
    with tempfile.TemporaryDirectory(prefix="ledgence-image-inventory-") as temporary:
        inventory = args.inventory or Path(temporary) / "runtime-inventory.json"
        image = inventory_from_image(args.image, args.platform, inventory) if args.image else None
        manifest = collect(inventory, args.output, workers=args.workers, image=image)
    print(json.dumps({"output": str(args.output), "complete": manifest["complete"],
                      "source_packages": manifest["source_package_count"]}))


if __name__ == "__main__":
    main()

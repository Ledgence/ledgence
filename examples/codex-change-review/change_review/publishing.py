"""Optional, reconciled GitHub publication for the fixed shipping example (MIT).

The operator supplies Git, an authenticated gh CLI, and an explicit sample
repository. No checkout, force push, merge, or global Git configuration is used.
An external push/PR is not an exactly-once effect: retry the unchanged bundle
after an uncertain outcome so its deterministic branch and commit can reconcile.
"""

import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import tempfile
from urllib.parse import urlencode

from .candidate import BASE_SHA256, validate_candidate
from .processes import ProcessError, collect

PUBLICATION_TIMEOUT = 120
COMMAND_TIMEOUT = 30
MAX_OUTPUT = 1024 * 1024


class PublicationError(ValueError):
    """Fixed diagnostics; CLI output and authentication details remain private."""


def validate_publication(publication):
    if type(publication) is not dict or publication.keys() != {"repository", "base_commit", "base_branch"}:
        raise PublicationError("publication requires repository, base_commit and base_branch")
    repository = publication["repository"]
    if (type(repository) is not str
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9][A-Za-z0-9._-]{0,99}", repository) is None
            or repository.lower().endswith(".git")):
        raise PublicationError("repository must be a GitHub owner/name for a dedicated sample repository")
    if repository.lower() == "ledgence/ledgence":
        raise PublicationError("the Ledgence product repository does not use pull requests")
    commit, branch = publication["base_commit"], publication["base_branch"]
    if type(commit) is not str or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise PublicationError("base_commit must be the full lowercase Git commit SHA")
    if (type(branch) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", branch) is None
            or ".." in branch or "//" in branch or any(part.startswith(".") or part.endswith((".", ".lock"))
                                                       for part in branch.split("/")) or branch.endswith("/")):
        raise PublicationError("base_branch must be an explicit Git branch name")
    return {"repository": repository.lower(), "base_commit": commit, "base_branch": branch}


async def _run(argv, *, cwd, env, deadline, action, data=None):
    """Bound each CLI and its output; retire the host if CLI teardown is uncertain."""
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise PublicationError("publication deadline exceeded; reconcile the unchanged bundle")
    try:
        code, output, _stderr = await collect(
            argv, cwd=cwd, environment=env, data=data or b"",
            timeout=min(COMMAND_TIMEOUT, remaining), stdout_limit=MAX_OUTPUT, stderr_limit=32 * 1024,
        )
    except (ProcessError, asyncio.CancelledError):
        # Git/gh may own authentication or transport descendants. Rust owns
        # this helper's process group; a normal application error could let
        # the helper be reused before those descendants have stopped.
        raise SystemExit("publication interrupted; retiring worker session; reconcile the unchanged bundle") from None
    if code:
        raise PublicationError(action + " failed; reconcile the unchanged bundle if publication was uncertain")
    return output


def _json(raw):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise PublicationError("GitHub returned an invalid response") from None


def _oid(raw):
    value = raw.decode("ascii", errors="replace").strip()
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise PublicationError("Git returned an invalid object identity")
    return value


class _Publisher:
    def __init__(self, directory, publication):
        self.directory, self.publication = directory, publication
        self.git, self.gh = shutil.which("git"), shutil.which("gh")
        if not self.git or not self.gh:
            raise PublicationError("install Git and GitHub CLI before enabling publication")
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith("GIT_") and key not in {"GH_DEBUG", "GH_REPO", "GH_HOST"}}
        self.env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0",
                        GH_HOST="github.com", GH_PROMPT_DISABLED="1", GH_PAGER="cat")
        self.deadline = asyncio.get_running_loop().time() + PUBLICATION_TIMEOUT
        self.remote = "https://github.com/" + publication["repository"] + ".git"

    async def git_command(self, *args, data=None, action="Git operation"):
        # The existing gh login authenticates HTTPS without reading or printing
        # its token. These credential options apply only to this subprocess.
        return await _run([self.git, "--no-pager", "-c", "credential.helper=", "-c",
                           "credential.https://github.com.helper=!" + shlex.quote(self.gh) + " auth git-credential",
                           "-c", "core.hooksPath=" + os.devnull, *args],
                          cwd=self.directory, env=self.env, deadline=self.deadline, action=action, data=data)

    async def gh_command(self, *args, action="GitHub operation"):
        return await _run([self.gh, *args], cwd=self.directory, env=self.env,
                          deadline=self.deadline, action=action)

    async def head(self, branch):
        raw = await self.git_command("ls-remote", "--heads", self.remote, "refs/heads/" + branch)
        if not raw:
            return None
        fields = raw.decode("ascii", errors="replace").strip().split("\t")
        if len(fields) != 2 or fields[1] != "refs/heads/" + branch:
            raise PublicationError("Git returned an unexpected branch identity")
        return _oid(fields[0].encode())

    async def commit(self, candidate, title):
        base = self.publication["base_commit"]
        await self.git_command("init", "--bare", "--quiet", "--object-format=sha1", ".")
        await self.git_command("fetch", "--quiet", "--depth=1", self.remote, base, action="base fetch")
        original = await self.git_command("cat-file", "blob", base + ":shipping.py")
        if hashlib.sha256(original).hexdigest() != BASE_SHA256 or candidate["base_sha256"] != BASE_SHA256:
            raise PublicationError("shipping.py at base_commit differs from the bundled example")
        entry = await self.git_command("ls-tree", "-z", base, "--", "shipping.py")
        if not re.fullmatch(rb"100(?:644|755) blob [0-9a-f]{40}\tshipping.py\x00", entry):
            raise PublicationError("shipping.py must be an ordinary tracked file")
        blob = _oid(await self.git_command("hash-object", "-w", "--stdin", data=candidate["source"].encode("utf-8")))
        await self.git_command("read-tree", base)
        await self.git_command("update-index", "--cacheinfo", entry[:6].decode() + "," + blob + ",shipping.py")
        tree = _oid(await self.git_command("write-tree"))
        timestamp = (await self.git_command("show", "-s", "--format=%ct", base)).decode().strip()
        if not timestamp.isdecimal():
            raise PublicationError("base commit timestamp is invalid")
        self.env.update(GIT_AUTHOR_NAME="ledgence-dev", GIT_AUTHOR_EMAIL="dev@ledgence.com",
                        GIT_COMMITTER_NAME="ledgence-dev", GIT_COMMITTER_EMAIL="dev@ledgence.com",
                        GIT_AUTHOR_DATE=timestamp + " +0000", GIT_COMMITTER_DATE=timestamp + " +0000")
        message = f"{title}\n\nLedgence change: {candidate['change_id']}\nCandidate-SHA256: {candidate['sha256']}\n"
        commit = _oid(await self.git_command("commit-tree", tree, "-p", base, data=message.encode("utf-8")))
        changed = await self.git_command("diff-tree", "--no-commit-id", "--name-only", "-z", "-r", base, commit)
        if changed != b"shipping.py\x00":
            raise PublicationError("the candidate must change shipping.py and no other file")
        return commit

    async def existing(self, branch, commit):
        repository = self.publication["repository"]
        query = urlencode({"state": "all", "head": repository.split("/")[0] + ":" + branch,
                           "base": self.publication["base_branch"], "per_page": 100})
        pulls = _json(await self.gh_command("api", "--hostname", "github.com", "--method", "GET",
                                          "repos/" + repository + "/pulls?" + query))
        if type(pulls) is not list or len(pulls) > 1:
            raise PublicationError("expected at most one pull request for this deterministic branch")
        if not pulls:
            return None
        pull = pulls[0]
        try:
            valid = (pull["head"]["ref"] == branch and pull["head"]["sha"] == commit
                     and pull["head"]["repo"]["full_name"].lower() == repository
                     and pull["base"]["ref"] == self.publication["base_branch"]
                     and pull["base"]["repo"]["full_name"].lower() == repository
                     and type(pull["number"]) is int and pull["number"] > 0
                     and pull["state"] in ("open", "closed")
                     and pull["html_url"].lower() == f"https://github.com/{repository}/pull/{pull['number']}")
        except (KeyError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise PublicationError("existing pull request does not match the approved candidate and target")
        return {"repository": repository, "branch": branch, "base_branch": self.publication["base_branch"],
                "base_commit": self.publication["base_commit"], "head_commit": commit,
                "number": pull["number"], "url": pull["html_url"],
                "state": "merged" if pull.get("merged_at") else pull["state"]}


async def publish(bundle, publication):
    """Publish only human-approved evidence; reconcile without force pushes."""
    from .steps import validate_bundle
    publication = validate_publication(publication)
    try:
        bundle = validate_bundle(bundle)
    except (ValueError, KeyError, TypeError):
        raise PublicationError("publication requires a valid approved evidence bundle") from None
    if bundle["status"] != "approved":
        raise PublicationError("only a human-approved bundle may be published")
    candidate = validate_candidate(bundle.get("candidate"))
    if bundle.get("change_id") != candidate["change_id"]:
        raise PublicationError("bundle and candidate change identities differ")
    pr = bundle.get("pull_request")
    if (type(pr) is not dict or type(pr.get("title")) is not str or not pr["title"].strip()
            or len(pr["title"].encode("utf-8")) > 256 or any(c in pr["title"] for c in "\r\n\x00")
            or type(pr.get("body")) is not str or not pr["body"].strip()
            or len(pr["body"].encode("utf-8")) > 16 * 1024 or "\x00" in pr["body"]):
        raise PublicationError("bundle must contain a bounded pull request title and body")
    key = hashlib.sha256(candidate["change_id"].encode("utf-8")).hexdigest()[:16]
    branch = "ledgence/change-" + key + "-" + candidate["sha256"][:16]
    with tempfile.TemporaryDirectory(prefix="ledgence-change-publish-") as temporary:
        directory = Path(temporary)
        publisher = _Publisher(directory, publication)
        commit = await publisher.commit(candidate, pr["title"])
        receipt = await publisher.existing(branch, commit)
        reconciled = receipt is not None
        if receipt is None:
            if await publisher.head(publication["base_branch"]) != publication["base_commit"]:
                raise PublicationError("base branch moved; prepare a newly reviewed publication target")
            current = await publisher.head(branch)
            if current not in (None, commit):
                raise PublicationError("publication branch contains a different commit; refusing to overwrite it")
            if current == commit:
                reconciled = True
            if current is None:
                try:
                    await publisher.git_command("push", "--porcelain", publisher.remote,
                                                commit + ":refs/heads/" + branch, action="branch push")
                except PublicationError:
                    if await publisher.head(branch) != commit:
                        raise PublicationError("branch push is uncertain; retry the unchanged bundle") from None
                    reconciled = True
            if await publisher.head(branch) != commit:
                raise PublicationError("published branch no longer matches the approved candidate")
            # Check again after pushing: a concurrent identical publication may
            # already have opened the PR, or the base may have moved meanwhile.
            receipt = await publisher.existing(branch, commit)
            if receipt is not None:
                reconciled = True
            else:
                if await publisher.head(publication["base_branch"]) != publication["base_commit"]:
                    raise PublicationError("base branch moved before PR creation; reconcile the existing branch")
                body = directory / "pull-request-body.md"
                body.write_text(pr["body"], encoding="utf-8")
                try:
                    await publisher.gh_command("pr", "create", "--repo", "github.com/" + publication["repository"],
                                               "--head", branch, "--base", publication["base_branch"],
                                               "--title", pr["title"], "--body-file", str(body),
                                               "--draft", "--no-maintainer-edit", action="pull request creation")
                except PublicationError:
                    reconciled = True
                receipt = await publisher.existing(branch, commit)
                if receipt is None:
                    raise PublicationError("pull request creation is uncertain; retry the unchanged bundle")
        result = copy.deepcopy(bundle)
        result["pull_request"]["url"] = receipt["url"]
        result["publication"] = {**receipt, "reconciled": reconciled}
        return result

"""Real local Git objects and a substituted GitHub CLI; no remote publication."""

import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from change_review import candidate, publishing, steps

GIT = shutil.which("git")
REPOSITORY = "sample-owner/pagination-demo"


def bundle():
    execution = {"provider": "codex", "model": "gpt-6-luna", "cli_version": "0.158.0",
                 "thread_id": "fixture-thread", "cli_invocations": 1,
                 "usage": {"input_tokens": 10, "cached_input_tokens": 0,
                           "output_tokens": 20, "reasoning_output_tokens": None}}
    source = candidate.BASE_SOURCE.replace("item_count // 100 + 1", "(item_count + 99) // 100")
    change = candidate.make_candidate("document-pages", source, "Avoid empty pages in document search", execution)
    measured = {"started_at_ms": 100, "finished_at_ms": 101, "pid": 1}
    identity = {"candidate_sha256": change["sha256"], **measured}
    return steps.assemble_bundle({
        "workflow_id": "wf-document-pages", "candidate": change,
        "comparison": {**identity, "cases": [
            {"item_count": item_count, "expected_pages": expected,
             "before": {"value": before, "error": None}, "after": {"value": expected, "error": None}}
            for item_count, before, expected in ((99, 1, 1), (100, 2, 1), (101, 2, 2))]},
        "tests": {**identity, "passed": True, "total": 6, "failures": 0, "errors": 0, "output": "6 fixture tests"},
        "review": {**identity, "verdict": "approve", "summary": "Fixture review", "findings": [], "execution": execution},
        "note": {**identity, "title": "No more empty search pages", "body": "Exactly 100 results now fit on one page.", "execution": execution},
        "decision": {"workflow_id": "wf-document-pages", "candidate_sha256": change["sha256"],
                     "outcome": "approved", "event_id": "approval-fixture"}})



def branch_name(value):
    return ("ledgence/change-" + hashlib.sha256(value["change_id"].encode()).hexdigest()[:16]
            + "-" + value["candidate"]["sha256"][:16])


class ValidationTests(unittest.TestCase):
    def test_publication_requires_an_explicit_dedicated_target(self):
        valid = {"repository": REPOSITORY, "base_commit": "a" * 40, "base_branch": "main"}
        self.assertEqual(publishing.validate_publication(valid), valid)
        invalid = [None, {}, {**valid, "extra": True},
                   *[{**valid, "repository": name} for name in
                     ("Ledgence/ledgence", "LEDGENCE/LEDGENCE", "https://github.com/owner/repo", "owner/repo.git",
                      "../repo", "owner/name/extra", "-owner/repo")],
                   *[{**valid, "base_commit": value} for value in ("main", "a" * 39, "A" * 40, None)],
                   *[{**valid, "base_branch": value} for value in ("--main", "a..b", "a//b", "a.lock", "a/", "a/.b", "a b", None)]]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(publishing.PublicationError):
                publishing.validate_publication(value)


@unittest.skipUnless(GIT, "Git is required for the publication fixture")
class PublishingTests(unittest.IsolatedAsyncioTestCase):
    def git(self, *args):
        return subprocess.check_output([GIT, "-C", str(self.remote), *args], env=self.git_env,
                                       stderr=subprocess.DEVNULL, timeout=10).decode().strip()

    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ledgence-publication-test-")
        self.remote = Path(self.temporary.name) / "sample"
        self.remote.mkdir()
        self.git_env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        self.git_env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                            GIT_AUTHOR_NAME="fixture", GIT_AUTHOR_EMAIL="fixture@example.invalid",
                            GIT_COMMITTER_NAME="fixture", GIT_COMMITTER_EMAIL="fixture@example.invalid",
                            GIT_AUTHOR_DATE="1600000000 +0000", GIT_COMMITTER_DATE="1600000000 +0000")
        self.git("init", "--quiet", "--initial-branch=main")
        (self.remote / "pagination.py").write_text(candidate.BASE_SOURCE)
        (self.remote / "README.md").write_text("Keep this file unchanged.\n")
        self.git("add", "pagination.py", "README.md")
        self.git("commit", "--quiet", "-m", "Base pagination fixture")
        self.base = self.git("rev-parse", "HEAD")
        self.publication = {"repository": REPOSITORY, "base_commit": self.base, "base_branch": "main"}
        self.calls, self.creations, self.pulls = [], [], []
        self.push_lost = self.create_lost = self.create_rejected = False
        self.advance_base_on_push = False
        self.original_run = publishing._run
        self.runner_patch = patch.object(publishing, "_run", side_effect=self.command)
        self.path_patch = patch.object(publishing.shutil, "which", side_effect=lambda name: GIT if name == "git" else "/fixture/gh")
        self.runner_patch.start()
        self.path_patch.start()

    async def asyncTearDown(self):
        self.runner_patch.stop()
        self.path_patch.stop()
        self.temporary.cleanup()

    async def command(self, argv, **kwargs):
        self.calls.append(list(argv))
        if argv[0] == GIT:
            rewritten = [str(self.remote) if arg == "https://github.com/" + REPOSITORY + ".git" else arg for arg in argv]
            result = await self.original_run(rewritten, **kwargs)
            if "push" in argv and self.advance_base_on_push:
                self.git("commit", "--quiet", "--allow-empty", "-m", "Concurrent base update")
            if "push" in argv and self.push_lost:
                self.push_lost = False
                raise publishing.PublicationError("branch push response lost")
            return result
        self.assertEqual(argv[0], "/fixture/gh")
        if argv[1] == "api":
            self.assertIn("--hostname", argv)
            self.assertEqual(argv[argv.index("--hostname") + 1], "github.com")
            query = parse_qs(urlsplit(argv[-1]).query)
            self.assertEqual(query["state"], ["all"])
            self.assertEqual(query["base"], ["main"])
            expected_head = REPOSITORY.split("/")[0] + ":" + branch_name(bundle())
            self.assertEqual(query["head"], [expected_head])
            return json.dumps(self.pulls).encode()
        self.assertEqual(argv[1:3], ["pr", "create"])
        options = {name: argv[argv.index(name) + 1] for name in ("--repo", "--head", "--base", "--title", "--body-file")}
        options["body"] = Path(options["--body-file"]).read_text()
        self.creations.append(options)
        self.assertIn("--no-maintainer-edit", argv)
        self.assertIn("--draft", argv)
        if self.create_rejected:
            raise publishing.PublicationError("creation rejected")
        head = self.git("rev-parse", "refs/heads/" + options["--head"])
        self.pulls.append({"number": 7, "html_url": "https://github.com/" + REPOSITORY + "/pull/7",
                           "state": "open", "merged_at": None,
                           "head": {"ref": options["--head"], "sha": head, "repo": {"full_name": REPOSITORY}},
                           "base": {"ref": "main", "sha": self.base, "repo": {"full_name": REPOSITORY}}})
        if self.create_lost:
            self.create_lost = False
            raise publishing.PublicationError("creation response lost")
        # This stdout is deliberately not a trustworthy publication receipt.
        return b"warning: use the API receipt instead\nhttps://example.invalid/not-the-pr\n"

    def writes(self):
        return [call for call in self.calls if "push" in call or call[1:3] == ["pr", "create"]]

    async def test_only_human_approved_reviewed_bundles_reach_any_cli(self):
        for value in (None, {}, {**bundle(), "status": "needs_changes"}, {**bundle(), "change_id": "other"},
                      {**bundle(), "decision": None}, {**bundle(), "status": "waiting_for_approval", "decision": None}):
            with self.subTest(status=value and value.get("status")), self.assertRaises(ValueError):
                await publishing.publish(value, self.publication)
        self.assertEqual(self.calls, [])

    async def test_publish_changes_only_the_reviewed_file_and_returns_verified_url(self):
        value = bundle()
        original = copy.deepcopy(value)
        result = await publishing.publish(value, self.publication)
        self.assertEqual(value, original)
        receipt = result["publication"]
        self.assertEqual(result["pull_request"]["url"], self.pulls[0]["html_url"])
        self.assertFalse(receipt["reconciled"])
        self.assertEqual(steps.validate_bundle(result), result)
        self.assertEqual(receipt["head_commit"], self.git("rev-parse", receipt["branch"]))
        self.assertEqual(self.git("rev-parse", receipt["head_commit"] + "^"), self.base)
        self.assertEqual(self.git("diff", "--name-only", self.base, receipt["head_commit"]), "pagination.py")
        self.assertEqual(self.git("show", receipt["head_commit"] + ":pagination.py") + "\n", value["candidate"]["source"])
        self.assertEqual(self.git("show", receipt["head_commit"] + ":README.md"), "Keep this file unchanged.")
        self.assertEqual(self.git("rev-parse", "main"), self.base)
        self.assertEqual(len(self.writes()), 2)
        self.assertFalse(any(arg.startswith("--force") for call in self.calls for arg in call))
        self.assertFalse(any("checkout" in call or "switch" in call or "setup-git" in call for call in self.calls))
        self.assertEqual(self.creations[0]["--title"], value["pull_request"]["title"])
        self.assertEqual(self.creations[0]["body"], value["pull_request"]["body"])

    async def test_exact_retry_reconstructs_same_commit_and_reuses_existing_pr(self):
        first = await publishing.publish(bundle(), self.publication)
        self.calls.clear()
        again = await publishing.publish(bundle(), self.publication)
        self.assertEqual(again["publication"]["head_commit"], first["publication"]["head_commit"])
        self.assertTrue(again["publication"]["reconciled"])
        self.assertEqual(len(self.pulls), 1)
        self.assertEqual(self.writes(), [])

    async def test_closed_or_merged_pr_is_not_recreated_after_base_advances(self):
        await publishing.publish(bundle(), self.publication)
        self.git("commit", "--quiet", "--allow-empty", "-m", "Advance base")
        self.calls.clear()
        self.pulls[0]["state"] = "closed"
        for merged, expected in ((None, "closed"), ("2026-09-28T12:00:00Z", "merged")):
            self.pulls[0]["merged_at"] = merged
            receipt = (await publishing.publish(bundle(), self.publication))["publication"]
            self.assertEqual(receipt["state"], expected)
        self.assertEqual(self.writes(), [])

    async def test_base_content_or_base_branch_drift_prevents_publication(self):
        (self.remote / "pagination.py").write_text("different pagination implementation\n")
        self.git("add", "pagination.py")
        self.git("commit", "--quiet", "-m", "Changed base")
        changed = self.git("rev-parse", "HEAD")
        with self.assertRaisesRegex(publishing.PublicationError, "differs from the bundled"):
            await publishing.publish(bundle(), {**self.publication, "base_commit": changed})
        with self.assertRaisesRegex(publishing.PublicationError, "base branch moved"):
            await publishing.publish(bundle(), self.publication)
        self.assertEqual(self.writes(), [])

    async def test_existing_branch_with_another_commit_is_never_overwritten(self):
        branch = branch_name(bundle())
        self.git("branch", branch, self.base)
        with self.assertRaisesRegex(publishing.PublicationError, "refusing to overwrite"):
            await publishing.publish(bundle(), self.publication)
        self.assertEqual(self.git("rev-parse", branch), self.base)
        self.assertEqual(self.writes(), [])

    async def test_base_move_after_push_leaves_branch_and_does_not_open_pr(self):
        self.advance_base_on_push = True
        with self.assertRaisesRegex(publishing.PublicationError, "base branch moved before PR"):
            await publishing.publish(bundle(), self.publication)
        self.assertNotEqual(self.git("rev-parse", branch_name(bundle())), self.base)
        self.assertEqual(self.creations, [])
        self.assertEqual(sum("push" in call for call in self.calls), 1)

    async def test_replacing_source_after_approval_cannot_reach_publication(self):
        value = bundle()
        value["candidate"] = candidate.make_candidate(value["change_id"], candidate.BASE_SOURCE,
                                                      "Unchanged source", value["candidate"]["execution"])
        with self.assertRaisesRegex(publishing.PublicationError, "valid approved evidence"):
            await publishing.publish(value, self.publication)
        self.assertEqual(self.writes(), [])

    async def test_lost_push_response_reconciles_remote_commit_without_another_push(self):
        self.push_lost = True
        result = await publishing.publish(bundle(), self.publication)
        self.assertTrue(result["publication"]["reconciled"])
        self.assertEqual(sum("push" in call for call in self.calls), 1)
        self.assertEqual(len(self.pulls), 1)

    async def test_lost_create_response_returns_observed_pr_without_another_create(self):
        self.create_lost = True
        result = await publishing.publish(bundle(), self.publication)
        self.assertTrue(result["publication"]["reconciled"])
        self.assertEqual(len(self.creations), 1)
        self.assertEqual(len(self.pulls), 1)

    async def test_retry_after_unconfirmed_create_reuses_pushed_branch(self):
        self.create_rejected = True
        with self.assertRaisesRegex(publishing.PublicationError, "creation is uncertain"):
            await publishing.publish(bundle(), self.publication)
        head = self.git("rev-parse", branch_name(bundle()))
        self.create_rejected = False
        self.calls.clear()
        result = await publishing.publish(bundle(), self.publication)
        self.assertEqual(result["publication"]["head_commit"], head)
        self.assertTrue(result["publication"]["reconciled"])
        self.assertFalse(any("push" in call for call in self.calls))

    async def test_mismatched_existing_receipt_never_claims_success_or_writes(self):
        await publishing.publish(bundle(), self.publication)
        saved = copy.deepcopy(self.pulls[0])
        invalid = [dict(saved, head={**saved["head"], "sha": "0" * 40}),
                   dict(saved, head={**saved["head"], "repo": {"full_name": "other/repository"}}),
                   dict(saved, base={**saved["base"], "ref": "different"}),
                   dict(saved, html_url="https://example.invalid/pull/7"), {}, None]
        for record in invalid:
            self.calls.clear()
            self.pulls = [record]
            with self.subTest(record=record), self.assertRaisesRegex(publishing.PublicationError, "does not match"):
                await publishing.publish(bundle(), self.publication)
            self.assertEqual(self.writes(), [])
        self.pulls = [saved, saved]
        with self.assertRaisesRegex(publishing.PublicationError, "at most one"):
            await publishing.publish(bundle(), self.publication)

    async def test_title_and_multiline_body_are_literal_cli_arguments_and_file_bytes(self):
        value = bundle()
        value["pull_request"].update(title="Handle $(not-a-command) and `literal` text",
                                     body="First line\n\nKeep $HOME and $(not-a-command) literally.\n")
        await publishing.publish(value, self.publication)
        self.assertEqual(self.creations[0]["--title"], value["pull_request"]["title"])
        self.assertEqual(self.creations[0]["body"], value["pull_request"]["body"])


class CommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_subprocess_errors_do_not_disclose_stdout_or_stderr(self):
        with patch.object(publishing, "collect", AsyncMock(return_value=(1, b"private-token", b"private-provider-body"))):
            with self.assertRaises(publishing.PublicationError) as error:
                await publishing._run(["gh"], cwd="/tmp", env={}, deadline=asyncio.get_running_loop().time() + 10,
                                      action="GitHub operation")
        self.assertNotIn("private", str(error.exception))

    async def test_interrupted_cli_retires_helper_for_worker_group_cleanup(self):
        for error in (publishing.ProcessError("private process output"), asyncio.CancelledError()):
            with patch.object(publishing, "collect", AsyncMock(side_effect=error)):
                with self.assertRaisesRegex(SystemExit, "retiring worker session"):
                    await publishing._run(["gh"], cwd="/tmp", env={}, deadline=asyncio.get_running_loop().time() + 10,
                                          action="GitHub operation")

    async def test_command_uses_remaining_deadline_and_bounded_output(self):
        with patch.object(publishing, "collect", AsyncMock(return_value=(0, b"result", b"ignored"))) as collect:
            result = await publishing._run(["gh"], cwd="/tmp", env={}, data=b"input",
                                          deadline=asyncio.get_running_loop().time() + 4, action="GitHub operation")
            self.assertEqual(result, b"result")
            self.assertGreater(collect.call_args.kwargs["timeout"], 0)
            self.assertLessEqual(collect.call_args.kwargs["timeout"], 4)
            self.assertEqual(collect.call_args.kwargs["stdout_limit"], publishing.MAX_OUTPUT)
            self.assertEqual(collect.call_args.kwargs["data"], b"input")
            collect.reset_mock()
            with self.assertRaisesRegex(publishing.PublicationError, "deadline exceeded"):
                await publishing._run(["gh"], cwd="/tmp", env={}, deadline=0, action="GitHub operation")
            collect.assert_not_called()


if __name__ == "__main__":
    unittest.main()

"""Exercise image patch and workflow fetch recipes offline, without Docker."""

import json
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/publish-interop-client.yml"
SETUP = (ROOT / "scripts/setup_picoquic_deps.sh").read_text()


def series_patches(dependency):
    # The same reading as provider_patches.sh read_patch_series.
    names = []
    for line in (ROOT / "patches" / dependency / "series").read_text().splitlines():
        line = re.sub(r"\s", "", line.split("#", 1)[0])
        if line:
            names.append(line)
    return names

def workflow_step(name):
    # Extract the repository's literal step/run blocks, not arbitrary YAML.
    text = WORKFLOW.read_text()
    start = text.index(f"      - name: {name}\n")
    end = text.find("\n      - name:", start + 1)
    return text[start:end if end != -1 else len(text)]


def step_script(step):
    lines = step.split("        run: |\n", 1)[1].splitlines()
    script = []
    for line in lines:
        if line and not line.startswith("          "):
            break
        script.append(line[10:])
    return "\n".join(script)


GIT_STUB = r'''import json, os, subprocess, sys
args = sys.argv[1:]
with open(os.environ["RECIPE_GIT_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if args[:1] == ["check-ref-format"]:
    sys.exit(subprocess.run([os.environ["RECIPE_REAL_GIT"], *args]).returncode)
command = args[2:] if args[:1] == ["-C"] else args
if os.environ.get("RECIPE_FAIL_GIT") == command[0]:
    sys.exit(23)
if command[0] == "rev-parse":
    print("0123456789abcdef")
'''


class OwnershipPatchRecipe(unittest.TestCase):
    dependency = "picoquic"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name).resolve()
        self.provider = self.root / self.dependency
        self.provider.mkdir()
        subprocess.run(["git", "init", "-q", str(self.provider)], check=True)
        self.names = series_patches(self.dependency)
        self.assertGreaterEqual(len(self.names), 2)
        patch_dir = self.root / "libmoq/patches" / self.dependency
        patch_dir.mkdir(parents=True)
        (patch_dir / "series").write_text("".join(n + "\n" for n in self.names), encoding="ascii")
        scripts = self.root / "libmoq/scripts"
        scripts.mkdir()
        shutil.copy(ROOT / "scripts/provider_patches.sh", scripts / "provider_patches.sh")
        self.sources = []
        self.patches = []
        for i, name in enumerate(self.names):
            source = self.provider / f"part{i}.c"
            source.write_text("borrowed\n", encoding="ascii")
            patch = patch_dir / name
            patch.write_text(
                f"diff --git a/part{i}.c b/part{i}.c\n"
                f"--- a/part{i}.c\n+++ b/part{i}.c\n"
                "@@ -1 +1 @@\n-borrowed\n+owned\n", encoding="ascii",
            )
            self.sources.append(source)
            self.patches.append(patch)
        self.source = self.sources[0]
        self.command = " ".join(shlex.quote(a) for a in self.dockerfile_apply()).replace(
            "/src", str(self.root))

    def dockerfile_apply(self):
        # The Dockerfile's provider_patches.sh invocation for this provider.
        lines = (ROOT / "tools/moq-interop-client/Dockerfile").read_text().splitlines()
        start = next(i for i, line in enumerate(lines) if "provider_patches.sh apply" in line)
        command = []
        for line in lines[start:]:
            command.append(line.removesuffix("\\"))
            if not line.endswith("\\"):
                break
        steps = [shlex.split(s) for s in " ".join(command).removeprefix("RUN ").split("&&")]
        matching = [s for s in steps if s[-1] == f"/src/{self.dependency}"]
        self.assertEqual(len(matching), 1, steps)
        return matching[0]

    def run_recipe(self, env=None):
        return subprocess.run(
            ["/bin/sh", "-c", self.command], capture_output=True, text=True,
            env=env, timeout=30,
        )

    def diagnostic(self, index, suffix):
        return f"[provider_patches] {self.dependency} patch {self.names[index]} {suffix}"

    def states(self):
        return [s.read_text() for s in self.sources]

    def test_recipe_applies_the_tracked_series(self):
        self.assertEqual(self.dockerfile_apply(), [
            "bash", "/src/libmoq/scripts/provider_patches.sh", "apply",
            f"/src/libmoq/patches/{self.dependency}", f"/src/{self.dependency}"])
        self.assertIn(f'read_patch_series "$repo_root/patches/{self.dependency}/series"', SETUP)
        for name in self.names:
            self.assertTrue((ROOT / "patches" / self.dependency / name).is_file(), name)

    def test_clean_and_already_applied(self):
        for suffix in ("", "already applied"):
            result = self.run_recipe()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            expected = "".join(
                (self.diagnostic(i, suffix) if suffix else
                 f"[provider_patches] applied {self.dependency} patch {self.names[i]}") + "\n"
                for i in range(len(self.names)))
            self.assertEqual(result.stderr, expected)
            self.assertEqual(self.states(), ["owned\n"] * len(self.names))

    def test_applied_prefix_completes(self):
        for count in range(1, len(self.names)):
            with self.subTest(count=count):
                for i, source in enumerate(self.sources):
                    source.write_text("owned\n" if i < count else "borrowed\n", encoding="ascii")
                result = self.run_recipe()
                self.assertEqual(result.returncode, 0, result.stderr)
                for i in range(count):
                    self.assertIn(self.diagnostic(i, "already applied"), result.stderr)
                self.assertEqual(self.states(), ["owned\n"] * len(self.names))

    def test_out_of_order_application_is_refused_unchanged(self):
        for index in range(1, len(self.names)):
            with self.subTest(index=index):
                expected = ["owned\n" if i == index else "borrowed\n"
                            for i in range(len(self.names))]
                for source, text in zip(self.sources, expected):
                    source.write_text(text, encoding="ascii")
                result = self.run_recipe()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("conflicts", result.stderr)
                self.assertEqual(self.states(), expected)

    def test_conflict_is_unchanged(self):
        for index in range(len(self.names)):
            with self.subTest(index=index):
                expected = ["conflict\n" if i == index else "borrowed\n"
                            for i in range(len(self.names))]
                for source, text in zip(self.sources, expected):
                    source.write_text(text, encoding="ascii")
                result = self.run_recipe()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    f"ERROR: {self.dependency} patch {self.names[index]} conflicts", result.stderr)
                self.assertEqual(self.states(), expected)

    def test_missing_second_patch_fails(self):
        self.patches[1].unlink()
        result = self.run_recipe()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"missing {self.dependency} patch", result.stderr)
        self.assertEqual(self.states(), ["borrowed\n"] * len(self.names))

    def test_apply_failure_is_not_masked_by_next_patch(self):
        bindir = self.root / "bin"
        bindir.mkdir()
        shim = bindir / "git"
        # Fail only the real (non-trial, non-check) apply of the chosen patch.
        shim.write_text(
            '#!/bin/sh\nif [ "$1" = -C ] && [ "$2" = "$RECIPE_PROVIDER" ] && '
            '[ "$3" = apply ] && [ "$4" = "$RECIPE_FAIL_PATCH" ]; then exit 23; fi\n'
            'exec "$RECIPE_REAL_GIT" "$@"\n', encoding="ascii",
        )
        shim.chmod(0o755)
        env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"],
                   RECIPE_REAL_GIT=shutil.which("git"), RECIPE_PROVIDER=str(self.provider),
                   RECIPE_FAIL_PATCH=str(self.patches[0]))
        result = self.run_recipe(env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"{self.dependency} patch {self.names[0]} failed to apply", result.stderr)
        self.assertEqual(self.states(), ["borrowed\n"] * len(self.names))

    def test_missing_repository(self):
        (self.provider / ".git").rename(self.root / "saved-git")
        result = self.run_recipe()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("is not its own git root", result.stderr)
        self.assertEqual(self.states(), ["borrowed\n"] * len(self.names))

    def test_enclosing_repository_is_not_provider(self):
        (self.provider / ".git").rename(self.root / "saved-git")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        result = self.run_recipe()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("is not its own git root", result.stderr)
        self.assertEqual(self.states(), ["borrowed\n"] * len(self.names))

    def test_unavailable_worktree_gitfile(self):
        (self.provider / ".git").rename(self.root / "saved-git")
        (self.provider / ".git").write_text("gitdir: /missing/host/worktree\n", encoding="ascii")
        result = self.run_recipe()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("is not its own git root", result.stderr)
        self.assertEqual(self.states(), ["borrowed\n"] * len(self.names))


    def test_linked_worktree_is_its_own_root(self):
        # A linked worktree's gitfile resolves to its own top level, so it is
        # a valid provider checkout.
        main = self.root / "provider-main"
        shutil.move(str(self.provider), str(main))
        git = ["git", "-C", str(main), "-c", "user.name=t", "-c", "user.email=t@t"]
        subprocess.run(git + ["add", "-A"], check=True)
        subprocess.run(git + ["commit", "-q", "-m", "base"], check=True)
        subprocess.run(git + ["worktree", "add", "-q", "--detach", str(self.provider)], check=True)
        self.assertTrue((self.provider / ".git").is_file())
        result = self.run_recipe()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.states(), ["owned\n"] * len(self.names))

class PicotlsPatchRecipe(OwnershipPatchRecipe):
    dependency = "picotls"


class WorkflowRecipe(unittest.TestCase):
    validate_name = "Validate picoquic + picotls refs"
    fetch_name = "Fetch picoquic + picotls (recurse submodules)"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        (self.root / "src").mkdir()
        (self.root / "src/libmoq").symlink_to(ROOT, target_is_directory=True)
        bindir = self.root / "bin"
        bindir.mkdir()
        shim = bindir / "git"
        shim.write_text(f"#!{sys.executable}\n" + GIT_STUB, encoding="ascii")
        shim.chmod(0o755)
        self.log = self.root / "git.jsonl"
        self.env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"],
                        RECIPE_REAL_GIT=shutil.which("git"), RECIPE_GIT_LOG=str(self.log),
                        GITHUB_ENV=str(self.root / "github-env"),
                        PICOQUIC_REPO="https://invalid.example/picoquic",
                        PICOTLS_REPO="https://invalid.example/picotls")

    def refs(self, step, overrides=None):
        refs = {}
        for dep in ("picoquic", "picotls"):
            pattern = rf"{dep.upper()}_REF: \$\{{\{{ github.event.inputs.{dep}_ref \|\| '([^']+)' \}}\}}"
            match = re.search(pattern, step)
            self.assertIsNotNone(match)
            refs[dep.upper() + "_REF"] = (overrides or {}).get(dep) or match.group(1)
        return refs

    def run_steps(self, overrides=None, fail=None):
        env = dict(self.env)
        if fail:
            env["RECIPE_FAIL_GIT"] = fail
        result = None
        for name in (self.validate_name, self.fetch_name):
            step = workflow_step(name)
            env.update(self.refs(step, overrides))
            result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step_script(step)],
                                    cwd=self.root, env=env, text=True, capture_output=True, timeout=10)
            if result.returncode:
                break
        return result

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_all_defaults_match_setup_pins(self):
        text = WORKFLOW.read_text()
        for dep in ("picoquic", "picotls"):
            pin = re.search(rf'^{dep.upper()}_REF="\$\{{{dep.upper()}_REF:-([0-9a-f]{{40}})\}}"', SETUP, re.M).group(1)
            manual = re.search(rf"      {dep}_ref:\n(.*?)(?=\n      \w|\n\S)", text, re.S).group(1)
            self.assertIn(f"default: '{pin}'", manual)
            self.assertIn(pin, re.search(r"description: (.*)", manual).group(1))
            for name in (self.validate_name, self.fetch_name):
                self.assertEqual(self.refs(workflow_step(name))[dep.upper() + "_REF"], pin)

    def test_validation_precedes_network_and_auth(self):
        text = WORKFLOW.read_text()
        self.assertLess(text.index(self.validate_name), text.index(self.fetch_name))
        self.assertLess(text.index(self.validate_name), text.index("Log in to GHCR"))
        for name in (self.validate_name, self.fetch_name):
            self.assertNotIn("${{", step_script(workflow_step(name)))

    def test_fetch_defaults_and_explicit_overrides(self):
        for overrides in (None, {"picoquic": "feature/reviewed", "picotls": "v1.2.3"},
                          {"picoquic": "1" * 40, "picotls": "2" * 40}):
            with self.subTest(overrides=overrides):
                self.log.unlink(missing_ok=True)
                result = self.run_steps(overrides)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, "")
                expected = []
                refs = self.refs(workflow_step(self.fetch_name), overrides)
                for dep in ("picoquic", "picotls"):
                    expected.append(["check-ref-format", "--branch", refs[dep.upper() + "_REF"]])
                for dep in ("picoquic", "picotls"):
                    prefix = ["-C", f"src/{dep}"]
                    expected.extend([
                        ["init", "-q", f"src/{dep}"],
                        prefix + ["remote", "add", "origin", self.env[dep.upper() + "_REPO"]],
                        prefix + ["fetch", "--depth", "1", "origin", refs[dep.upper() + "_REF"]],
                        prefix + ["checkout", "-q", "--detach", "FETCH_HEAD"],
                        prefix + ["submodule", "update", "--init", "--recursive", "--depth", "1"],
                    ])
                for dep in ("picoquic", "picotls"):
                    expected.append(["-C", f"src/{dep}", "rev-parse", "--short", "HEAD"])
                self.assertEqual(self.calls(), expected)

    def test_invalid_override_never_fetches(self):
        for bad in ("--upload-pack=bad", "$(touch BAD)", "good\nbad", "bad..ref", "a; touch BAD"):
            for dep in ("picoquic", "picotls"):
                with self.subTest(bad=bad, dep=dep):
                    self.log.unlink(missing_ok=True)
                    result = self.run_steps({dep: bad})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("invalid", result.stderr)
                    self.assertTrue(all(c[0] == "check-ref-format" for c in self.calls()))
                    self.assertFalse((self.root / "BAD").exists())

    def test_git_failures_stop_fetch_step(self):
        for failure in ("init", "remote", "fetch", "checkout", "submodule", "rev-parse"):
            with self.subTest(failure=failure):
                self.log.unlink(missing_ok=True)
                result = self.run_steps(fail=failure)
                self.assertNotEqual(result.returncode, 0)
                last = self.calls()[-1]
                self.assertEqual(last[2] if last[0] == "-C" else last[0], failure)


if __name__ == "__main__":
    unittest.main()

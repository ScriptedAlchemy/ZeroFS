"""Execute the native Jepsen setup steps without package or network side effects."""

import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/jepsen-ha.yml"


class NativeJepsenDepsTests(unittest.TestCase):
    def test_dependency_bundle_works_in_both_runtime_homes(self):
        workflow = WORKFLOW.read_text()
        install = re.findall(
            r"      - name: Install the resolved Jepsen dependencies\n"
            r"        run: \|\n((?:          [^\n]*\n|\n)+)", workflow,
        )
        self.assertEqual(len(install), 2)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            launcher = root / "mock-launcher"
            launcher.write_text(r"""#!/usr/bin/env bash
set -eu
case "$*" in
  version)
    mkdir -p "$LEIN_HOME/self-installs"
    echo jar > "$LEIN_HOME/self-installs/leiningen-2.12.0-standalone.jar";;
  deps)
    repo=$(sed -n 's/.*:local-repo "\([^"]*\)".*/\1/p' "$LEIN_HOME/profiles.clj")
    mkdir -p "$repo"
    touch "$repo/dependency.jar";;
  '-o deps')
    test -e "${LEIN_HOME%/*}/.m2/repository/dependency.jar";;
  *) exit 1;;
esac
""")
            curl = root / "curl"
            curl.write_text(r"""#!/usr/bin/env bash
if [[ " ${*} " == *" -o /dev/null "* ]]; then
  echo "${!#}" >> "$RUNNER_TEMP/probes"
  [[ "${FAIL_PROBE:-}" != "${!#}" ]]
  exit $?
fi
printf '%s\n' "$@" > "$RUNNER_TEMP/curl-args"
cp "$MOCK_LAUNCHER" "${!#}"
""")
            curl.chmod(0o755)
            sudo = root / "sudo"
            sudo.write_text('#!/usr/bin/env bash\nexec "$@"\n')
            sudo.chmod(0o755)
            env = dict(os.environ, COPYFILE_DISABLE="1", PATH=f"{root}:{os.environ['PATH']}",
                       GITHUB_WORKSPACE=str(ROOT), RUNNER_TEMP=directory, MOCK_LAUNCHER=str(launcher))
            first_central = "https://repo.maven.apache.org/maven2/org/clojure/clojure/1.11.1/clojure-1.11.1.pom"
            env["FAIL_PROBE"] = first_central
            (root / "jepsen-ha-deps.tar").write_bytes(b"incomplete cache entry")
            rebuilt = subprocess.run(["bash", str(ROOT / "scripts/ci/prepare-jepsen-deps.sh")],
                                     env=env, check=True, capture_output=True, text=True)
            self.assertIn("Discarding incomplete cached Jepsen bundle", rebuilt.stderr)
            self.assertEqual((root / "probes").read_text().splitlines(), [
                first_central,
                "https://repo1.maven.org/maven2/org/clojure/clojure/1.11.1/clojure-1.11.1.pom",
                "https://repo.clojars.org/jepsen/jepsen/0.3.7/jepsen-0.3.7.pom",
            ])
            # A fresh invocation with the bundle can verify it entirely offline.
            curl.write_text("#!/usr/bin/env bash\nexit 22\n")
            subprocess.run(["bash", str(ROOT / "scripts/ci/prepare-jepsen-deps.sh")],
                           env=env, check=True)
            download = (root / "curl-args").read_text().splitlines()
            for option in ("--fail", "--retry-all-errors"):
                self.assertIn(option, download)
            for option, value in (("--connect-timeout", "15"), ("--max-time", "60"),
                                  ("--retry-max-time", "180"), ("--retry", "3")):
                self.assertEqual(download[download.index(option) + 1], value)
            with tarfile.open(root / "jepsen-ha-deps.tar") as bundle:
                self.assertEqual(set(bundle.getnames()), {
                    "lein", ".lein/self-installs", ".lein/self-installs/leiningen-2.12.0-standalone.jar",
                    ".m2/repository", ".m2/repository/dependency.jar",
                })
            for index, step in enumerate(install):
                for client in ("native", "fuse"):
                    with self.subTest(matrix=index, client=client):
                        home = root / f"runner-{index}-{client}"
                        root_home = root / f"root-{index}-{client}"
                        home.mkdir()
                        root_home.mkdir()
                        step_cmd = step.replace("${{ matrix.mount-client }}", client)
                        step_cmd = step_cmd.replace("-C /root", f'-C "{root_home}"')
                        subprocess.run(["bash", "-ec", textwrap.dedent(step_cmd)],
                                       env=dict(env, HOME=str(home)), check=True)
                        runtime = home if client == "native" else root_home
                        self.assertTrue(os.access(runtime / "lein", os.X_OK))
                        self.assertTrue((runtime / ".m2/repository/dependency.jar").is_file())

    def setup_steps(self):
        blocks = re.findall(
            r"      - name: Install guest JDK and gnuplot\n"
            r"(?:        [^\n]*\n)*?        run: \|\n((?:          [^\n]*\n|\n)+)",
            WORKFLOW.read_text(),
        )
        self.assertEqual(len(blocks), 2, "exercise both the HA and durability jobs")
        return [textwrap.dedent(block) for block in blocks]

    def run_setup(self, step, fail_phase=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "commands.jsonl"
            sudo = root / "sudo"
            sudo.write_text('#!/usr/bin/env bash\nexec "$@"\n')
            sudo.chmod(0o755)
            tool = root / "apt-get"
            tool.write_text('''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["COMMAND_LOG"], "a") as log:
    log.write(json.dumps([name, args, os.environ.get("DEBIAN_FRONTEND")]) + "\\n")
if name == "apt-get":
    phase = "update" if "update" in args else "install"
    if os.environ.get("FAIL_PHASE") == phase:
        # apt normally accepts a transient index failure with a warning.
        # Strict refresh must turn that warning into a failure before install.
        if phase == "install" or "APT::Update::Error-Mode=any" in args:
            sys.exit(100)
''')
            tool.chmod(0o755)
            environment = dict(
                os.environ,
                PATH=f"{root}:{os.environ['PATH']}",
                GITHUB_WORKSPACE=str(ROOT),
                COMMAND_LOG=str(log),
                FAIL_PHASE=fail_phase,
            )
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step],
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            commands = [json.loads(line) for line in log.read_text().splitlines()]
            return result, commands

    def test_success_installs_headless_graph_tools_with_bounded_transport(self):
        for step in self.setup_steps():
            with self.subTest(step=step):
                result, commands = self.run_setup(step)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual([name for name, _, _ in commands],
                                 ["apt-get", "apt-get"])
                for _, arguments, frontend in commands[:2]:
                    self.assertEqual(frontend, "noninteractive")
                    for option in ("Acquire::Retries=3", "Acquire::http::Timeout=30",
                                   "Acquire::https::Timeout=30"):
                        self.assertIn(option, arguments)
                packages = commands[1][1]
                for package in ("gnuplot-nox", "fonts-liberation", "openjdk-17-jdk-headless"):
                    self.assertIn(package, packages)
                self.assertNotIn("gnuplot", packages)
                self.assertIn("--no-install-recommends", packages)

    def test_failed_index_refresh_stops_before_installing_or_downloading_lein(self):
        for step in self.setup_steps():
            with self.subTest(step=step):
                result, commands = self.run_setup(step, fail_phase="update")
                self.assertEqual(result.returncode, 100, result.stderr)
                self.assertEqual([name for name, _, _ in commands], ["apt-get"])

    def test_failed_package_install_stops_setup_without_retrying_the_step(self):
        for step in self.setup_steps():
            with self.subTest(step=step):
                result, commands = self.run_setup(step, fail_phase="install")
                self.assertEqual(result.returncode, 100, result.stderr)
                self.assertEqual([name for name, _, _ in commands], ["apt-get", "apt-get"])

    def test_failed_launcher_download_stops_before_bootstrap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            curl = root / "curl"
            curl.write_text('#!/usr/bin/env bash\n[[ " $* " == *" -o /dev/null "* ]] && exit 0\nexit 22\n')
            curl.chmod(0o755)
            result = subprocess.run(
                ["bash", str(ROOT / "scripts/ci/prepare-jepsen-deps.sh")],
                env=dict(os.environ, GITHUB_WORKSPACE=str(ROOT), RUNNER_TEMP=directory,
                         PATH=f"{root}:{os.environ['PATH']}"), check=False,
            )
            self.assertEqual(result.returncode, 22)
            self.assertFalse((root / "jepsen-ha-deps.tar").exists())


if __name__ == "__main__":
    unittest.main()

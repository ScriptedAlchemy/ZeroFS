import json
import re
import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "csi-docker.yml"
BASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docker.yml"
EVENT_PLANNER = REPO_ROOT / "scripts" / "ci" / "container_build_event.py"
PUBLIC_BASE = (
    "ghcr.io/barre/zerofs@"
    "sha256:3ec09262cba72ec84d12a9f64e796a2cb607c962c2b94e3704c14cde7102e2e0"
)


class CsiDockerWorkflowTests(unittest.TestCase):
    def test_tagless_build_uses_a_pinned_public_base(self) -> None:
        workflow = WORKFLOW.read_text()
        self.assertIn(PUBLIC_BASE, workflow)
        self.assertNotIn("scriptedalchemy/zerofs:latest", workflow.lower())

    def test_container_workflows_split_validation_from_publish_permissions(
        self,
    ) -> None:
        self.assertIn(
            "python3 -m unittest scripts.tests.test_csi_docker_workflow",
            WORKFLOW.read_text(),
        )
        for path in (WORKFLOW, BASE_WORKFLOW):
            with self.subTest(workflow=path.name):
                workflow = path.read_text()
                self.assertIn("scripts/ci/container_build_event.py", workflow)
                self.assertEqual(workflow.count("packages: write"), 1)
                validate = workflow.split("validate-image:", maxsplit=1)[1]
                validate, publish = validate.split("publish-image:", maxsplit=1)
                self.assertNotIn("packages: write", validate)
                self.assertIn("packages: write", publish)
                self.assertIn("format('refs/tags/{0}', inputs.tag)", publish)
                self.assertNotIn("needs.plan.outputs", workflow)

        csi_publish = WORKFLOW.read_text().split("publish-image:", maxsplit=1)[1]
        self.assertLess(
            csi_publish.index("- name: Log in to Container Registry"),
            csi_publish.index("- name: Resolve release base image"),
        )

    def test_external_actions_and_cross_are_immutable(self) -> None:
        for path in (WORKFLOW, BASE_WORKFLOW):
            with self.subTest(workflow=path.name):
                workflow = path.read_text()
                mutable = re.findall(r"uses:\s+[^\s#]+@(v\d+|stable|main)\b", workflow)
                self.assertEqual(mutable, [])
        self.assertRegex(
            WORKFLOW.read_text(),
            r"cargo install cross .* --rev [0-9a-f]{40} --locked",
        )


class ContainerBuildEventTests(unittest.TestCase):
    def run_planner(self, event_name: str, ref: str, input_tag: str = "") -> dict:
        result = subprocess.run(
            [
                sys.executable,
                str(EVENT_PLANNER),
                "--event-name",
                event_name,
                "--ref",
                ref,
                "--input-tag",
                input_tag,
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_event_matrix_selects_exact_source_and_publish_mode(self) -> None:
        cases = [
            (
                "pull_request",
                "refs/pull/31/merge",
                "",
                {
                    "publish": False,
                    "source_ref": "refs/pull/31/merge",
                    "release_tag": "",
                },
            ),
            (
                "workflow_dispatch",
                "refs/heads/develop",
                "",
                {
                    "publish": False,
                    "source_ref": "refs/heads/develop",
                    "release_tag": "",
                },
            ),
            (
                "workflow_dispatch",
                "refs/heads/develop",
                "v2.2.2",
                {
                    "publish": True,
                    "source_ref": "refs/tags/v2.2.2",
                    "release_tag": "v2.2.2",
                },
            ),
            (
                "push",
                "refs/tags/v2.2.2",
                "",
                {
                    "publish": True,
                    "source_ref": "refs/tags/v2.2.2",
                    "release_tag": "v2.2.2",
                },
            ),
        ]
        for event_name, ref, input_tag, expected in cases:
            with self.subTest(event_name=event_name, ref=ref, input_tag=input_tag):
                self.assertEqual(self.run_planner(event_name, ref, input_tag), expected)

    def test_event_matrix_rejects_non_tag_publish_requests(self) -> None:
        for event_name, ref, input_tag in (
            ("workflow_dispatch", "refs/heads/develop", "develop"),
            ("workflow_dispatch", "refs/heads/develop", "2.2.2"),
            ("push", "refs/heads/develop", ""),
        ):
            with self.subTest(event_name=event_name, ref=ref, input_tag=input_tag):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(EVENT_PLANNER),
                        "--event-name",
                        event_name,
                        "--ref",
                        ref,
                        "--input-tag",
                        input_tag,
                    ],
                    cwd=REPO_ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()

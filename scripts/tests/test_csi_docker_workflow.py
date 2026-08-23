import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "csi-docker.yml"
DOCKERFILE = REPO_ROOT / "zerofs" / "zerofs-csi" / "Dockerfile"


class CsiDockerWorkflowTests(unittest.TestCase):
    def test_tagless_build_uses_the_published_dockerfile_base(self) -> None:
        dockerfile = DOCKERFILE.read_text()
        workflow = WORKFLOW.read_text()

        dockerfile_base = re.search(
            r"^ARG ZEROFS_IMAGE=(?P<image>\S+)$", dockerfile, re.MULTILINE
        )
        self.assertIsNotNone(dockerfile_base)

        resolve_step = workflow.split("- name: Resolve base image", maxsplit=1)[1]
        resolve_step = resolve_step.split("- name:", maxsplit=1)[0]
        tagless_base = re.search(
            r'^\s*else\s*$.*?^\s*base="(?P<image>[^"]+)"$',
            resolve_step,
            re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(tagless_base)

        self.assertEqual(
            tagless_base.group("image"),
            dockerfile_base.group("image"),
            "PR and tagless CSI builds must use the published base image",
        )


if __name__ == "__main__":
    unittest.main()

"""Artifact admission must finish before the smoke CLI prepares a guest.

Run the real shell/Python resolver with temporary artifact bytes. The catalog
fixture supplies only the expected target identity; PATH deliberately omits
cpio so a valid manifest stops at the boot-preparation boundary. No server,
QEMU, privileged operation, or network is involved.
"""

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


SMOKE = Path(__file__).resolve().parents[1] / "module-smoke.sh"
BOOT_BOUNDARY = "required command not found: cpio"


class ModuleSmokeManifestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.script = self.root / "repo/kernel/ci/module-smoke.sh"
        self.script.parent.mkdir(parents=True)
        shutil.copyfile(SMOKE, self.script)
        catalog = self.root / "repo/packaging/kernel/kernel-targets.py"
        catalog.parent.mkdir(parents=True)
        catalog.write_text(
            "#!/bin/sh\ncase \"$5\" in\n"
            "arch) printf '%s\\n' x86_64 ;;\n"
            "kernel_release) printf '%s\\n' 6.18.0-test ;;\n"
            "*) exit 2 ;;\nesac\n",
            encoding="utf-8",
        )
        catalog.chmod(0o755)
        self.catalog_manifest = self.root / "catalog.json"
        self.catalog_manifest.write_text("{}", encoding="utf-8")
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        self.payloads = {
            name: (name + " fixture\n").encode()
            for name in ("kernel", "zerofs.ko", "busybox", "dependency.ko", "boot.ko")
        }
        for name, payload in self.payloads.items():
            (self.artifacts / name).write_bytes(payload)
        self.manifest = {
            "schema_version": 2,
            "target_id": "fixture-x86_64",
            "kernel_release": "6.18.0-test",
            "arch": "x86_64",
            "kernel_image": "kernel",
            "module": "zerofs.ko",
            "boot_busybox": "busybox",
            "module_dependencies": ["dependency.ko"],
            "boot_modules": ["boot.ko"],
            "sha256": {
                name: hashlib.sha256(payload).hexdigest()
                for name, payload in self.payloads.items()
            },
        }
        self.bash = shutil.which("bash")
        self.assertIsNotNone(self.bash)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name in ("dirname", "realpath"):
            executable = shutil.which(name)
            self.assertIsNotNone(executable, f"{name} is required by the production script")
            (self.bin / name).symlink_to(executable)
        (self.bin / "python3").symlink_to(sys.executable)

    def invoke(self, manifest):
        artifact_manifest = self.artifacts / "artifact.json"
        artifact_manifest.write_text(
            manifest if isinstance(manifest, str) else json.dumps(manifest),
            encoding="utf-8",
        )
        result = subprocess.run(
            [self.bash, str(self.script), "package", str(self.catalog_manifest),
             "fixture-x86_64", str(self.artifacts), str(self.root / "unused-server")],
            env={**os.environ, "PATH": str(self.bin)},
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        for name, payload in self.payloads.items():
            self.assertEqual((self.artifacts / name).read_bytes(), payload)
        return result

    def assert_rejected(self, manifest, diagnostic):
        result = self.invoke(manifest)
        self.assertIn(diagnostic, result.stderr)
        self.assertNotIn(BOOT_BOUNDARY, result.stderr,
                         "a failed resolver must not admit partial stdout as a complete manifest")

    def test_valid_manifests_reach_the_boot_preparation_boundary(self):
        for optional in (True, False):
            with self.subTest(optional=optional):
                manifest = copy.deepcopy(self.manifest)
                if not optional:
                    manifest.update(module_dependencies=[], boot_modules=[])
                result = self.invoke(manifest)
                self.assertIn(BOOT_BOUNDARY, result.stderr)
                self.assertNotIn("invalid artifact", result.stderr)

    def test_invalid_dependency_list_cannot_be_ignored_after_core_inputs(self):
        self.assert_rejected({**self.manifest, "module_dependencies": {}},
                             "module lists must be arrays")

    def test_invalid_boot_list_cannot_be_ignored_after_core_inputs(self):
        self.assert_rejected({**self.manifest, "boot_modules": "boot.ko"},
                             "module lists must be arrays")

    def test_late_dependency_checksum_failure_refuses_partial_output(self):
        manifest = copy.deepcopy(self.manifest)
        (self.artifacts / "bad.ko").write_bytes(b"wrong bytes")
        manifest["module_dependencies"].append("bad.ko")
        manifest["sha256"]["bad.ko"] = "0" * 64
        self.assert_rejected(manifest, "SHA-256 mismatch for module_dependencies[1]")

    def test_late_boot_path_escape_refuses_partial_output(self):
        manifest = copy.deepcopy(self.manifest)
        (self.root / "outside.ko").write_bytes(b"outside artifact root")
        manifest["boot_modules"].append("../outside.ko")
        self.assert_rejected(manifest, "boot_modules[1] escapes the artifact directory")

    def test_malformed_json_refuses_before_boot_preparation(self):
        self.assert_rejected("{broken", "invalid artifact manifest")


if __name__ == "__main__":
    unittest.main()

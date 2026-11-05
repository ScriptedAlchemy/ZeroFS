import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kernel_targets.discovery import apt


AS_OF = datetime(2026, 7, 30, tzinfo=timezone.utc)
STRICT_UPDATE = "APT::Update::Error-Mode=any"


class AptRefreshTests(unittest.TestCase):
    def test_failed_refresh_stops_before_candidate_selection(self):
        for distro in ("ubuntu", "debian"):
            for failed_update in (1, 2):
                with self.subTest(distro=distro, failed_update=failed_update):
                    updates = []

                    def run(arguments):
                        if arguments[0] == "apt-get" and arguments[-1] == "update":
                            updates.append(arguments)
                            # APT normally reports transient fetch failures as
                            # warnings with status zero unless strict mode is set.
                            if (
                                len(updates) == failed_update
                                and STRICT_UPDATE in arguments
                            ):
                                raise RuntimeError("index fetch failed")
                        return ""

                    runner = Mock()
                    runner.run.side_effect = run
                    channel = {
                        "distro": distro,
                        "discovery": {"suite": "stable", "selector": "linux-image-generic"},
                    }
                    with patch.object(
                        apt, "_candidate",
                        side_effect=AssertionError("queried incomplete indexes"),
                    ) as candidate:
                        with self.assertRaisesRegex(RuntimeError, "index fetch failed"):
                            apt.observe(channel, {}, AS_OF, runner)
                        candidate.assert_not_called()
                    self.assertEqual(len(updates), failed_update)
                    self.assertEqual(
                        runner.replace_apt_sources.call_count, failed_update - 1,
                    )

    def test_successful_refresh_preserves_pinned_signed_sources(self):
        for distro in ("ubuntu", "debian"):
            with self.subTest(distro=distro):
                runner = Mock()
                runner.run.return_value = ""
                channel = {
                    "distro": distro,
                    "discovery": {"suite": "stable", "selector": "linux-image-generic"},
                }
                with patch.object(
                    apt, "_candidate", side_effect=RuntimeError("candidate reached"),
                ) as candidate:
                    with self.assertRaisesRegex(RuntimeError, "candidate reached"):
                        apt.observe(channel, {}, AS_OF, runner)
                    candidate.assert_called_once_with(
                        runner, "linux-image-generic", "stable",
                    )
                updates = [
                    call.args[0] for call in runner.run.call_args_list
                    if call.args[0][-1] == "update"
                ]
                self.assertEqual(len(updates), 2)
                self.assertTrue(all(STRICT_UPDATE in command for command in updates))
                source = runner.replace_apt_sources.call_args.args[1]
                self.assertIn("20260730T000000Z", source)
                self.assertIn(f"/usr/share/keyrings/{distro}-archive-keyring.gpg", source)
                self.assertNotIn("trusted=yes", source)


if __name__ == "__main__":
    unittest.main()

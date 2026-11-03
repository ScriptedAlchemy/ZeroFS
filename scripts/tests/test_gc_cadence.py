"""Pure GC rewrite regressions; no pilot lifecycle or mounted filesystem."""
import math
import tomllib
import unittest

from scripts.vm100_pilot.gc_cadence import CADENCE_KEYS, rewrite_gc_cadence


class GcCadenceTests(unittest.TestCase):
    def test_missing_keys_follow_complete_root_multiline_values(self):
        values = (
            '[\n  ["gc"],\n  ["writeback"],\n]',
            '"""\n[writeback]\ninterval_secs = 777\n"""',
            "'''\n[gc]\n'''",
            '"""text ending in a quote""""',
        )
        for value in values:
            with self.subTest(value=value):
                source = f'gc.note = {value}\n[writeback]\ninterval_secs=90\n'
                result = rewrite_gc_cadence(source, 3600)
                expected = tomllib.loads(source)
                expected["gc"].update(dict.fromkeys(CADENCE_KEYS, 3600))
                self.assertEqual(tomllib.loads(result), expected)
                self.assertIn(f'gc.note = {value}\n', result)
                self.assertEqual(rewrite_gc_cadence(result, 3600), result)

    def test_unicode_separators_preserve_physical_line_boundaries(self):
        for newline in ("\n", "\r\n"):
            for separator in ("\u0085", "\u2028", "\u2029"):
                with self.subTest(newline=newline, separator=separator):
                    value = f'"""first{separator}[gc]{separator}last"""'
                    source = f'gc.note = {value}{newline}[writeback]{newline}interval_secs=90'
                    result = rewrite_gc_cadence(source, 3600)
                    expected = tomllib.loads(source)
                    expected["gc"].update(dict.fromkeys(CADENCE_KEYS, 3600))
                    self.assertEqual(tomllib.loads(result), expected)
                    self.assertIn(f'gc.note = {value}{newline}', result)
                    self.assertEqual(rewrite_gc_cadence(result, 3600), result)

    def test_equals_signs_in_quoted_keys_are_preserved(self):
        source = 'gc."note=literal" = "untouched"\n["other=table"]\nvalue=4\n'
        result = rewrite_gc_cadence(source, 3600)
        expected = tomllib.loads(source)
        expected["gc"].update(dict.fromkeys(CADENCE_KEYS, 3600))
        self.assertEqual(tomllib.loads(result), expected)
        self.assertIn('gc."note=literal" = "untouched"\n', result)

    def test_unrelated_nan_does_not_fail_output_validation(self):
        source = '[other]\nvalue=nan\n'
        result = rewrite_gc_cadence(source, 3600)
        self.assertTrue(result.startswith(source))
        values = tomllib.loads(result)
        self.assertTrue(math.isnan(values["other"]["value"]))
        self.assertEqual(values["gc"], dict.fromkeys(CADENCE_KEYS, 3600))

    def test_cadence_must_be_an_integer(self):
        for interval in (299, 86401, True, 3600.0):
            with self.subTest(interval=interval), self.assertRaises(ValueError):
                rewrite_gc_cadence('[gc]\n', interval)
        for value in ('true', '"60"', '60.0', '[]'):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "integer"):
                rewrite_gc_cadence('[gc]\ninterval_secs=' + value, 3600)


if __name__ == "__main__":
    unittest.main()

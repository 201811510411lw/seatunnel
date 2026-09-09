import csv
import tempfile
import unittest
from pathlib import Path

from summarize import summarize


class SummarizeTest(unittest.TestCase):
    def setUp(self):
        self.samples = [
            {
                "parallelism": parallelism,
                "fields": fields,
                "variant": variant,
                "fork": fork,
                "batch": batch,
                "rows": 4096,
                "iterations": 300000,
                "sha": "baseline" if variant == "baseline" else "candidate",
                "ns_per_row": 100 if variant == "baseline" else 80,
                "allocated_bytes_per_row": 200 if variant == "baseline" else 100,
                "checksum": 17,
                "route_checksum": 23,
            }
            for parallelism in (1, 2)
            for fields in (4, 32)
            for variant in ("baseline", "candidate")
            for fork in range(1, 4)
            for batch in range(1, 4)
        ]

    def evaluate(self, forks=3, batches=3):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.csv"
            with path.open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=self.samples[0].keys())
                writer.writeheader()
                writer.writerows(self.samples)
            return summarize(path, forks, batches)

    def test_reports_four_separate_scenarios(self):
        for scenario in self.evaluate():
            self.assertEqual(0.8, scenario["time_ratio"])
            self.assertEqual(0.5, scenario["allocation_ratio"])
            self.assertTrue(scenario["within_threshold"])
            self.assertFalse(scenario["needs_noise_review"])

    def test_rejects_duplicate_and_missing_batches(self):
        self.samples.append(self.samples[0].copy())
        with self.assertRaises(ValueError):
            self.evaluate()
        self.samples.pop()
        self.samples.pop()
        with self.assertRaises(ValueError):
            self.evaluate()

    def test_rejects_invalid_measurements_and_mixed_inputs(self):
        changes = {
            "ns_per_row": ["nan", "inf", "-1"],
            "allocated_bytes_per_row": ["nan", "inf", "0"],
            "iterations": [17],
            "rows": [17],
            "sha": ["another-revision"],
            "checksum": [18],
            "variant": ["unknown"],
        }
        for field, values in changes.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    original = self.samples[0][field]
                    self.samples[0][field] = value
                    with self.assertRaises(ValueError):
                        self.evaluate()
                    self.samples[0][field] = original

    def test_does_not_pass_regression(self):
        for sample in self.samples:
            if sample["variant"] == "candidate":
                sample["ns_per_row"] = 106
        self.assertTrue(all(not row["within_threshold"] for row in self.evaluate()))

    def test_rejects_uniformly_truncated_measurement_plan(self):
        with self.assertRaises(ValueError):
            self.evaluate(forks=5, batches=7)
        with self.assertRaises(ValueError):
            self.evaluate(forks=3, batches=7)

    def test_rejects_empty_or_invalid_checksums_even_when_all_match(self):
        for value in ("", " ", "nan", str(1 << 63), str(-(1 << 63) - 1)):
            with self.subTest(value=value):
                for sample in self.samples:
                    sample["checksum"] = value
                with self.assertRaises(ValueError):
                    self.evaluate()
        for sample in self.samples:
            sample["checksum"] = 0
        self.assertEqual(4, len(self.evaluate()))


if __name__ == "__main__":
    unittest.main()

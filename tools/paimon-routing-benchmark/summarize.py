import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def summarize(path, expected_forks, expected_batches):
    if min(expected_forks, expected_batches) < 3:
        raise ValueError("At least three forks and batches required")
    groups = defaultdict(list)
    identifiers = set()
    configurations = set()
    revisions = defaultdict(set)
    with path.open(newline="") as source:
        for row in csv.DictReader(source):
            if row["variant"] not in ("baseline", "candidate"):
                raise ValueError("Unknown variant")
            key = (int(row["parallelism"]), int(row["fields"]), row["variant"])
            identifier = key + (int(row["fork"]), int(row["batch"]))
            if identifier in identifiers or min(identifier[-2:]) < 1:
                raise ValueError("Duplicate or invalid fork/batch")
            identifiers.add(identifier)
            configurations.add((int(row["rows"]), int(row["iterations"])))
            revisions[row["variant"]].add(row["sha"])
            for checksum in ("checksum", "route_checksum"):
                value = int(row[checksum])
                if not -(1 << 63) <= value < (1 << 63):
                    raise ValueError("Checksum outside signed 64-bit range")
            for metric in ("ns_per_row", "allocated_bytes_per_row"):
                value = float(row[metric])
                if not math.isfinite(value) or value <= 0:
                    raise ValueError("Nonfinite or nonpositive measurement")
            groups[key].append(row)
    if len(configurations) != 1 or any(min(config) < 1 for config in configurations):
        raise ValueError("Mixed or invalid input configurations")
    if any(len(shas) != 1 for shas in revisions.values()):
        raise ValueError("Mixed revisions within variant")
    scenarios = []
    for parallelism, fields in sorted({key[:2] for key in groups}):
        scenario = {"parallelism": parallelism, "fields": fields}
        checksums = set()
        fork_sets = []
        batch_sets = []
        for variant in ("baseline", "candidate"):
            samples = groups[(parallelism, fields, variant)]
            forks = defaultdict(list)
            for sample in samples:
                checksums.add((sample["checksum"], sample["route_checksum"]))
                forks[int(sample["fork"])].append(sample)
            if set(forks) != set(range(1, expected_forks + 1)):
                raise ValueError("Forks do not match the declared measurement plan")
            fork_sets.append(set(forks))
            for batches in forks.values():
                batch_set = {int(sample["batch"]) for sample in batches}
                if batch_set != set(range(1, expected_batches + 1)):
                    raise ValueError("Batches do not match the declared measurement plan")
                batch_sets.append(batch_set)
            result = {"forks": len(forks), "batches": len(samples)}
            for metric in ("ns_per_row", "allocated_bytes_per_row"):
                medians = [
                    statistics.median(float(sample[metric]) for sample in batches)
                    for _, batches in sorted(forks.items())
                ]
                if any(value <= 0 for value in medians):
                    raise ValueError("Nonpositive timing or allocation measurement")
                result[metric] = statistics.median(medians)
                result[metric + "_fork_medians"] = medians
                result[metric + "_relative_spread"] = (
                    max(medians) - min(medians)
                ) / result[metric]
            scenario[variant] = result
        if (
            len(checksums) != 1
            or fork_sets[0] != fork_sets[1]
            or any(batch_set != batch_sets[0] for batch_set in batch_sets)
        ):
            raise ValueError("Unmatched checksums, forks or batches")
        scenario["time_ratio"] = (
            scenario["candidate"]["ns_per_row"] / scenario["baseline"]["ns_per_row"]
        )
        scenario["allocation_ratio"] = (
            scenario["candidate"]["allocated_bytes_per_row"]
            / scenario["baseline"]["allocated_bytes_per_row"]
        )
        scenario["within_threshold"] = (
            scenario["time_ratio"] <= 1.05 and scenario["allocation_ratio"] <= 1
        )
        scenario["needs_noise_review"] = any(
            scenario[variant]["ns_per_row_relative_spread"] > 0.15
            for variant in ("baseline", "candidate")
        )
        scenarios.append(scenario)
    if {(row["parallelism"], row["fields"]) for row in scenarios} != {
        (1, 4), (1, 32), (2, 4), (2, 32)
    }:
        raise ValueError("Expected all four scenarios")
    return scenarios


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize paired Paimon benchmark forks")
    parser.add_argument("results", type=Path)
    parser.add_argument("--forks", type=int, required=True)
    parser.add_argument("--batches", type=int, required=True)
    arguments = parser.parse_args()
    print(json.dumps(summarize(arguments.results, arguments.forks, arguments.batches), indent=2))

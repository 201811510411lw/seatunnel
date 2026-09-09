#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 4 ]]; then
    echo "Usage: bash $0 BASELINE_CLASSPATH_FILE CANDIDATE_CLASSPATH_FILE CANDIDATE_SHA OUTPUT_DIR" >&2
    exit 2
fi

baseline_sha=180c2d730409bfc7a89670423c6b6f84c377ed91
baseline_cp=$(cat "$1")
candidate_cp=$(cat "$2")
candidate_sha=$3
output=$4
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
java_home=${JAVA_HOME:-/home/lsym005226/project/jdk1.8.0_202}
forks=${FORKS:-5}
warmups=${WARMUPS:-8}
batches=${BATCHES:-7}
iterations=${ITERATIONS:-1000000}
rows=${ROWS:-4096}
heap=${HEAP:-1g}
jit_mode=${JIT_MODE:-blocking-c2}

[[ $candidate_sha =~ ^[0-9a-f]{40}$ ]] || { echo "CANDIDATE_SHA must be a full SHA" >&2; exit 2; }
for value in "$forks" "$warmups" "$batches" "$iterations" "$rows"; do
    [[ $value =~ ^[1-9][0-9]*$ ]] || { echo "Counts must be positive integers" >&2; exit 2; }
done
[[ $heap =~ ^[1-9][0-9]*[mMgG]$ ]] || { echo "HEAP must look like 1g or 512m" >&2; exit 2; }
[[ -n $baseline_cp && -n $candidate_cp ]] || { echo "Empty classpath" >&2; exit 2; }
for classpath in "$baseline_cp" "$candidate_cp"; do
    [[ $classpath != *$'\n'* && $classpath != *'*'* && $classpath != :* && $classpath != *: && $classpath != *::* ]] || { echo "Use one explicit classpath line, no wildcards or empty entries" >&2; exit 2; }
    IFS=: read -r -a entries <<< "$classpath"
    for entry in "${entries[@]}"; do
        [[ $entry = /* && -e $entry ]] || { echo "Classpath entries must be existing absolute paths: $entry" >&2; exit 2; }
    done
done
"$java_home/bin/java" -version 2>&1 | grep -q 'version "1.8.' || { echo "JDK 8 required" >&2; exit 2; }
[[ -z ${JAVA_TOOL_OPTIONS:-}${_JAVA_OPTIONS:-}${JDK_JAVA_OPTIONS:-} ]] || { echo "Unset injected JVM option variables" >&2; exit 2; }

exec 9>"${TMPDIR:-/tmp}/paimon-routing-benchmark-${UID}.lock"
flock -n 9 || { echo "Another benchmark is running; never run baseline/candidate concurrently" >&2; exit 2; }
mkdir -- "$output"
output=$(cd -- "$output" && pwd)
mkdir "$output/classes" "$output/tmp"
printf '%s\n' "$baseline_cp" > "$output/baseline.classpath"
printf '%s\n' "$candidate_cp" > "$output/candidate.classpath"
affinity=()
if [[ -n ${CPUSET:-} ]]; then
    affinity=(taskset -c "$CPUSET")
fi
java_options=(-Xms"$heap" -Xmx"$heap" -XX:+UseParallelGC -Dfile.encoding=UTF-8 -Djava.io.tmpdir="$output/tmp")
case "$jit_mode" in
    blocking-c2) java_options+=(-Xbatch -XX:-TieredCompilation -XX:ParallelGCThreads=1) ;;
    tiered) ;;
    *) echo "JIT_MODE must be blocking-c2 or tiered" >&2; exit 2 ;;
esac
{
    date --iso-8601=seconds
    uname -a
    "$java_home/bin/java" -version
    printf 'baseline_sha=%s\ncandidate_sha=%s\n' "$baseline_sha" "$candidate_sha"
    printf 'java_home=%s\ncpuset=%s\njit_mode=%s\nforks=%s\nwarmups=%s\nbatches=%s\niterations=%s\nrows=%s\n' \
        "$java_home" "${CPUSET:-unbound}" "$jit_mode" "$forks" "$warmups" "$batches" "$iterations" "$rows"
    printf 'jvm_options='; printf '%q ' "${java_options[@]}"; printf '\n'
    sha256sum "$script_dir/PaimonRoutingBenchmark.java" "$script_dir/run.sh"
    lscpu
    taskset -pc $$
    for setting in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor /sys/fs/cgroup/cpu.max; do
        if [[ -r $setting ]]; then printf '%s=' "$setting"; cat "$setting"; fi
    done
} > "$output/environment.txt" 2>&1

"$java_home/bin/javac" -encoding UTF-8 -source 8 -target 8 -proc:none \
    -cp "$baseline_cp" -d "$output/classes" "$script_dir/PaimonRoutingBenchmark.java"
sha256sum "$output"/classes/benchmark/*.class > "$output/harness.sha256"

for ((fork=1; fork<=forks; fork++)); do
    for parallelism in 1 2; do
        for fields in 4 32; do
            variants=(baseline candidate)
            if (( fork % 2 == 0 )); then variants=(candidate baseline); fi
            for variant in "${variants[@]}"; do
                classpath=$baseline_cp
                sha=$baseline_sha
                if [[ $variant == candidate ]]; then classpath=$candidate_cp; sha=$candidate_sha; fi
                name="$variant-p$parallelism-f$fields-fork$fork"
                printf '%s %s\n' "$(date --iso-8601=seconds)" "$name" >> "$output/order.txt"
                "${affinity[@]}" "$java_home/bin/java" "${java_options[@]}" \
                    -cp "$output/classes:$classpath" benchmark.PaimonRoutingBenchmark \
                    "$variant" "$sha" "$fork" "$parallelism" "$fields" "$rows" \
                    "$warmups" "$batches" "$iterations" > "$output/$name.csv" 2> "$output/$name.log"
            done
        done
    done
done
awk 'FNR == 1 && NR != 1 { next } { print }' "$output"/*-fork*.csv > "$output/results.csv"
printf 'Completed: %s/results.csv\n' "$output"

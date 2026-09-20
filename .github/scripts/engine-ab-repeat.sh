#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
: "${TEST_SELECTOR:?}"
: "${EXPECTED_SHA:?}"
: "${REPEATS:?}"
: "${GITHUB_WORKSPACE:?}"

case "$TEST_SELECTOR" in
  'CheckpointCoordinatorFailoverIT#testStreamJobFailsAfterCheckpointTriggerDispatchFailure'|'BackpressureSlowSinkIT#testCheckpointsKeepCompletingUnderSustainedBackpressure') ;;
  *) echo 'Refusing a selector outside the two authorized methods'; exit 2 ;;
esac
[[ "$REPEATS" =~ ^[1-5]$ ]]
[[ "$(git rev-parse HEAD)" == "$EXPECTED_SHA" ]]

evidence_dir="$GITHUB_WORKSPACE/evidence"
report_dir='seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/target/failsafe-reports'
mkdir -p "$evidence_dir"
{
  git rev-parse HEAD
  git status --short
  java -version
  ./mvnw -version
  docker version
  nproc
  free -m
  command -v python3
  python3 --version
} > "$evidence_dir/environment.txt" 2>&1

command=(./mvnw -T 1 -B verify -DskipUT=true -DskipIT=false
  -Dlicense.skipAddThirdParty=true -Dskip.ui=true --no-snapshot-updates
  -pl :connector-seatunnel-e2e-base -am -Pci "-Dit.test=$TEST_SELECTOR"
  -Dfailsafe.failIfNoSpecifiedTests=false -DfailIfNoTests=false)
printf '%q ' "${command[@]}" > "$evidence_dir/command.txt"
printf '\n' >> "$evidence_dir/command.txt"
overall=0
for ((iteration=1; iteration<=REPEATS; iteration++)); do
  round_dir="$evidence_dir/round-$iteration"
  mkdir -p "$round_dir"
  # Remove generated reports so a build failure cannot reuse an earlier test result.
  rm -rf -- "$report_dir"
  date -u +%FT%TZ > "$round_dir/started.txt"
  set +e
  "${command[@]}" 2>&1 | tee "$round_dir/maven.log"
  maven_rc=${PIPESTATUS[0]}
  set -e
  date -u +%FT%TZ > "$round_dir/finished.txt"
  printf '%s\n' "$maven_rc" > "$round_dir/maven-exit-code.txt"
  if [[ -d "$report_dir" ]]; then
    cp -a "$report_dir" "$round_dir/failsafe-reports"
  fi
  set +e
  python3 - "$round_dir" "$TEST_SELECTOR" "$maven_rc" <<'PY'
import json
import pathlib
import sys
import xml.etree.ElementTree as ET

directory = pathlib.Path(sys.argv[1])
expected_class, expected_method = sys.argv[2].split('#')
cases = []
for report in sorted((directory / 'failsafe-reports').glob('TEST-*.xml')):
    for case in ET.parse(report).getroot().iter('testcase'):
        cases.append({
            'class': case.get('classname'),
            'method': case.get('name'),
            'seconds': case.get('time'),
            'skipped': case.find('skipped') is not None,
            'failures': [node.get('message', '') for node in case.findall('failure')],
            'errors': [node.get('message', '') for node in case.findall('error')],
        })
valid = (len(cases) == 1
         and cases[0]['class'].split('.')[-1] == expected_class
         and cases[0]['method'] == expected_method
         and not cases[0]['skipped'])
failed = any(case['failures'] or case['errors'] for case in cases)
exit_code = int(sys.argv[3])
status = ('invalid_execution' if not valid else
          'test_failed' if failed else
          'build_failed' if exit_code else 'passed')
result = {'status': status, 'maven_exit_code': exit_code, 'cases': cases}
(directory / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
sys.exit(2 if status in ('invalid_execution', 'build_failed') else
         1 if status == 'test_failed' else 0)
PY
  result_rc=$?
  set -e
  if ((result_rc != 0)); then
    overall=1
  fi
  if ((result_rc > 1)); then
    echo 'Stopping: infrastructure/build failure or selected method did not execute exactly once.'
    break
  fi
done
exit "$overall"

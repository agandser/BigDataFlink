#!/usr/bin/env bash
set -euo pipefail

until curl -fsS http://flink-jobmanager:8081/overview >/dev/null; do
  echo "Waiting for Flink JobManager..."
  sleep 5
done

until [ "$(curl -fsS http://flink-jobmanager:8081/taskmanagers | python -c "import sys, json; print(len(json.load(sys.stdin).get('taskmanagers', [])))")" -ge 1 ]; do
  echo "Waiting for Flink TaskManager..."
  sleep 5
done

flink run -d -m flink-jobmanager:8081 -py /opt/flink/jobs/streaming_star_schema_job.py

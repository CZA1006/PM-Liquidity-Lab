#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=.

# 使用 RUN_ID 区分每次跑
TS=$(date +"%Y%m%d-%H%M%S")
export RUN_ID="probe-${TS}"

python -m app.probe --duration-sec 600
echo
echo "Done. Check: data/runs/${RUN_ID}/probe_report.json"
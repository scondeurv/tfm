#!/usr/bin/env bash
# Detached runner for the remaining campaign phases. Survives client/session
# disconnect (launch with: setsid nohup ./run_rest_campaign.sh &). Self-resilient:
# preflight gate + cleanup + per-cell skip + health scan are built into the
# orchestrator, so it runs to completion unattended and records any failed cells.
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export CAMPAIGN_ROOT="${CAMPAIGN_ROOT:-experiment_data/cloudlab_campaigns/campaign-unified-20260606T164415Z}"
export OW_HOST="${OW_HOST:-10.99.125.88}"
export ALGORITHMS="${ALGORITHMS:-lp bfs sssp pagerank}"
LOG="${CAMPAIGN_ROOT}/logs/run_rest.log"
mkdir -p "${CAMPAIGN_ROOT}/logs"

ts() { date -u +%H:%M:%S; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }

# 1. Wait for any in-flight quick-phase run to finish (avoid concurrent sweeps).
log "waiting for in-flight run_cloudlab_campaign to finish (if any)"
while pgrep -f run_cloudlab_campaign.py >/dev/null 2>&1; do
  sleep 20
done
log "no in-flight campaign; starting remaining phases on ${CAMPAIGN_ROOT}"

# 2. Run the heavy phases sequentially. A hard failure in one phase stops the
#    chain (per-cell failures do NOT — they are recorded and the sweep continues).
for PH in size cost report; do
  log "########## PHASE=${PH} start ##########"
  if ! PHASE="${PH}" bash campaigns/launch_campaign_v3.sh >>"$LOG" 2>&1; then
    log "PHASE ${PH} HARD-FAILED (rc=$?); stopping chain. Resume later: PHASE=${PH} bash campaigns/launch_campaign_v3.sh"
    exit 1
  fi
  log "########## PHASE=${PH} done ##########"
done

log "REMAINING CAMPAIGN COMPLETE — see ${CAMPAIGN_ROOT}/report/"

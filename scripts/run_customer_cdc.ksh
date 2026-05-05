#!/bin/ksh
# Customer CDC Pipeline — detects inserts/updates/deletes in customer master
# Triggered by: JOB_CUSTOMER_CDC (AutoSys, runs every 4 hours)

set -e

. /opt/abinitio/setenv.ksh

GRAPH_DIR="${AI_PROJECT_DIR}/graphs/customer"
PSET_DIR="${AI_PROJECT_DIR}/psets"
LOG_DIR="${AI_LOG_DIR}/customer/$(date +%Y%m%d_%H%M)"
RUN_TS=$(date +%Y-%m-%d_%H:%M:%S)

mkdir -p "${LOG_DIR}"

echo "[${RUN_TS}] Starting customer CDC pipeline" >> "${LOG_DIR}/run.log"

# Step 1: Snapshot current state from source
air sandbox run \
  "${GRAPH_DIR}/snapshot_customer.mp" \
  -pset "${PSET_DIR}/customer_snapshot.pset" \
  -param RUN_TIMESTAMP="${RUN_TS}" \
  -log "${LOG_DIR}/snapshot.log"

# Step 2: Compare with previous snapshot (hash-based CDC)
air sandbox run \
  "${GRAPH_DIR}/cdc_detect_customer.mp" \
  -pset "${PSET_DIR}/customer_cdc.pset" \
  -param RUN_TIMESTAMP="${RUN_TS}" \
  -partition 8 \
  -log "${LOG_DIR}/cdc_detect.log"

# Step 3: Apply changes to target
air sandbox run \
  "${GRAPH_DIR}/apply_customer_changes.mp" \
  -pset "${PSET_DIR}/customer_apply.pset" \
  -param RUN_TIMESTAMP="${RUN_TS}" \
  -log "${LOG_DIR}/apply.log"

# Step 4: Generate audit trail
air sandbox run \
  "${GRAPH_DIR}/audit_customer_changes.mp" \
  -pset "${PSET_DIR}/customer_audit.pset" \
  -param RUN_TIMESTAMP="${RUN_TS}" \
  -log "${LOG_DIR}/audit.log"

echo "[$(date)] Customer CDC pipeline completed" >> "${LOG_DIR}/run.log"
exit 0

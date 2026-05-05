#!/bin/ksh
# Daily Orders Pipeline — Ab Initio batch orchestration
# Triggered by AutoSys job: JOB_DAILY_ORDERS_LOAD
# Runs: orders extraction → CDC → staging → production load

set -e

. /opt/abinitio/setenv.ksh

GRAPH_DIR="${AI_PROJECT_DIR}/graphs/orders"
PSET_DIR="${AI_PROJECT_DIR}/psets"
LOG_DIR="${AI_LOG_DIR}/orders/$(date +%Y%m%d)"
BATCH_DATE=$(date +%Y-%m-%d)

mkdir -p "${LOG_DIR}"

echo "[$(date)] Starting daily orders pipeline for ${BATCH_DATE}" >> "${LOG_DIR}/run.log"

# Phase 1: Extract from source systems
echo "[$(date)] Phase 1: Source extraction" >> "${LOG_DIR}/run.log"
air sandbox run \
  "${GRAPH_DIR}/extract_orders.mp" \
  -pset "${PSET_DIR}/orders_extract.pset" \
  -param BATCH_DATE="${BATCH_DATE}" \
  -log "${LOG_DIR}/extract.log" \
  2>&1 | tee -a "${LOG_DIR}/run.log"

RC=$?
if [ $RC -ne 0 ]; then
  echo "[$(date)] FATAL: Extraction failed with RC=${RC}" >> "${LOG_DIR}/run.log"
  exit $RC
fi

# Phase 2: CDC detection
echo "[$(date)] Phase 2: CDC processing" >> "${LOG_DIR}/run.log"
air sandbox run \
  "${GRAPH_DIR}/cdc_orders.mp" \
  -pset "${PSET_DIR}/orders_cdc.pset" \
  -param BATCH_DATE="${BATCH_DATE}" \
  -partition 4 \
  -log "${LOG_DIR}/cdc.log" \
  2>&1 | tee -a "${LOG_DIR}/run.log"

# Phase 3: Load to staging
echo "[$(date)] Phase 3: Staging load" >> "${LOG_DIR}/run.log"
air sandbox run \
  "${GRAPH_DIR}/load_staging_orders.mp" \
  -pset "${PSET_DIR}/orders_staging.pset" \
  -param BATCH_DATE="${BATCH_DATE}" \
  -log "${LOG_DIR}/staging.log" \
  2>&1 | tee -a "${LOG_DIR}/run.log"

# Phase 4: Production rollover
echo "[$(date)] Phase 4: Production load" >> "${LOG_DIR}/run.log"
air sandbox run \
  "${GRAPH_DIR}/prod_rollover_orders.mp" \
  -pset "${PSET_DIR}/orders_prod.pset" \
  -param BATCH_DATE="${BATCH_DATE}" \
  -log "${LOG_DIR}/prod.log" \
  2>&1 | tee -a "${LOG_DIR}/run.log"

echo "[$(date)] Daily orders pipeline completed successfully" >> "${LOG_DIR}/run.log"
exit 0

#!/bin/ksh
# Ab Initio environment setup — sourced by all pipeline scripts
# Sets paths, credentials, and runtime configuration

export AI_HOME=/opt/abinitio
export AI_PROJECT_DIR=/data/projects/enterprise_etl
export AI_LOG_DIR=/data/logs/abinitio
export AI_SANDBOX_DIR=/data/sandbox
export AI_DATA_DIR=/data/raw
export AI_STAGING_DIR=/data/staging
export AI_ARCHIVE_DIR=/data/archive

# Co>Operating System paths
export AB_HOME=${AI_HOME}/coop
export PATH=${AB_HOME}/bin:${AI_HOME}/bin:${PATH}

# Database connections (resolved from PSET at runtime)
export AI_SOURCE_DB=ORACLE_PROD
export AI_TARGET_DB=TERADATA_DW
export AI_STAGING_DB=ORACLE_STG

# Partitioning defaults
export AI_DEFAULT_PARTITIONS=4
export AI_MAX_PARTITIONS=16

# Error handling
export AI_MAX_ERRORS=100
export AI_ERROR_ACTION=ABORT  # ABORT | CONTINUE | SKIP

# Checkpoint/restart
export AI_CHECKPOINT_ENABLED=true
export AI_CHECKPOINT_DIR=${AI_SANDBOX_DIR}/checkpoints

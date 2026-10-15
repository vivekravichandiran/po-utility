#!/usr/bin/env bash
# Usage: scripts/deploy.sh <databricks-cli-profile> [workspace_dir=/Shared/po_control]
# Imports po_worker and po_wrapper as Python notebooks (overwrites existing).
set -euo pipefail
PROFILE="${1:?profile name required}"
DEST="${2:-/Shared/po_control}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
databricks workspace mkdirs "$DEST" --profile "$PROFILE"
for nb in po_worker po_wrapper; do
  databricks workspace import "$DEST/$nb" --file "$HERE/notebooks/$nb.py" \
    --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"
  echo "deployed $DEST/$nb"
done

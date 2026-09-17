#!/usr/bin/env bash
# package.sh — build flight-alert-classifier.zip for import into IBM ADS Decision Center
#
# Usage:
#   cd ads-decision-service
#   bash package.sh
#
# Output:
#   ads-decision-service/flight-alert-classifier.zip
#
# The zip file has the standard ADS project layout expected by Decision Center's
# "Import" function:
#   src/main/resources/com/ibm/planetrack/
#       FlightAlertClassifier.dmn
#       types/AlertInput.dmn
#       types/AlertDecision.dmn
#       decisions/SeverityRules.dmn
#       decisions/ActionRules.dmn

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ZIP_NAME="flight-alert-classifier.zip"
ZIP_PATH="${SCRIPT_DIR}/${ZIP_NAME}"
SRC_DIR="${SCRIPT_DIR}/src"

# ── Preflight checks ────────────────────────────────────────────────────────
if ! command -v zip &>/dev/null; then
  echo "ERROR: 'zip' is not installed. On RHEL/Fedora: sudo dnf install zip" >&2
  exit 1
fi

if [ ! -d "${SRC_DIR}" ]; then
  echo "ERROR: src/ directory not found. Run from the ads-decision-service/ directory." >&2
  exit 1
fi

# Verify every required DMN file is present before zipping
REQUIRED=(
  "src/main/resources/com/ibm/planetrack/FlightAlertClassifier.dmn"
  "src/main/resources/com/ibm/planetrack/types/AlertInput.dmn"
  "src/main/resources/com/ibm/planetrack/types/AlertDecision.dmn"
  "src/main/resources/com/ibm/planetrack/decisions/SeverityRules.dmn"
  "src/main/resources/com/ibm/planetrack/decisions/ActionRules.dmn"
)
for f in "${REQUIRED[@]}"; do
  if [ ! -f "${SCRIPT_DIR}/${f}" ]; then
    echo "ERROR: required file missing: ${f}" >&2
    exit 1
  fi
done

# ── Build the zip ────────────────────────────────────────────────────────────
rm -f "${ZIP_PATH}"
(cd "${SCRIPT_DIR}" && zip -r "${ZIP_NAME}" src/)

echo ""
echo "✓ Created: ${ZIP_PATH}"
echo ""
echo "Next steps:"
echo "  1. In ADS Decision Center, open the 'plane-track' space"
echo "  2. Click '+ New decision service' → 'Import'"
echo "  3. Select: ${ZIP_NAME}"
echo "  4. Deploy to the embedded deployment space"
echo "  5. Copy the execute URL for the classifyFlightAlert operation"
echo "  See ads-decision-service/README.md for detailed instructions."

#!/usr/bin/env bash
#
# End-to-end local test of the semi-automated NCBI transfer pipeline.
#
# Runs all three phases locally using a user-managed MinIO instance and
# user-supplied staging directories.  This lets you inspect the S3 store
# and intermediate files between phases, rerun individual phases, and
# test retransfers over existing records.
#
# Prerequisites:
#   - A running MinIO (or S3-compatible) instance with MINIO_ENDPOINT_URL,
#     MINIO_ACCESS_KEY, and MINIO_SECRET_KEY exported in the environment.
#   - docker (or podman symlinked/aliased to docker)
#   - uv (https://docs.astral.sh/uv/)
#
# Usage:
#   ./scripts/ncbi/test_e2e_local.sh --input-dir DIR --output-dir DIR \
#       [--limit N] [--prefix-from NNN] [--prefix-to NNN] [--phase N]
#
# Example:
#   mkdir -p /tmp/ncbi_input /tmp/ncbi_output
#   export MINIO_ENDPOINT_URL=http://localhost:9100
#   export MINIO_ACCESS_KEY=minioadmin
#   export MINIO_SECRET_KEY=minioadmin
#   ./scripts/ncbi/test_e2e_local.sh \
#       --input-dir /tmp/ncbi_input --output-dir /tmp/ncbi_output \
#       --limit 2 --prefix-from 000 --prefix-to 000
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LIMIT="${LIMIT:-2}"
PREFIX_FROM=""
PREFIX_TO=""
INPUT_DIR=""
OUTPUT_DIR=""
PHASE=""                 # empty = run all phases
DOWNLOADER_IMAGE="ncbi-sync:e2e-test"
BUCKET="cdm-lake"
STORE_PREFIX="tenant-general-warehouse/kbase/datasets/ncbi/"

# ── Parse arguments ──────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --limit)       LIMIT="$2"; shift 2 ;;
        --prefix-from) PREFIX_FROM="$2"; shift 2 ;;
        --prefix-to)   PREFIX_TO="$2"; shift 2 ;;
        --input-dir)   INPUT_DIR="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$2"; shift 2 ;;
        --phase)       PHASE="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ── Validate environment ────────────────────────────────────────────────
if [[ -z "${MINIO_ENDPOINT_URL:-}" ]]; then
    echo "ERROR: MINIO_ENDPOINT_URL is not set."
    echo "Start a MinIO container first — see the README for instructions."
    exit 1
fi
if [[ -z "$INPUT_DIR" || -z "$OUTPUT_DIR" ]]; then
    echo "ERROR: --input-dir and --output-dir are required."
    echo ""
    echo "Usage: $0 --input-dir DIR --output-dir DIR [--limit N] [--phase N]"
    exit 1
fi

mkdir -p "$INPUT_DIR" "$OUTPUT_DIR"

echo "=== NCBI Transfer Pipeline – Local E2E Test ==="
echo "  minio:       $MINIO_ENDPOINT_URL"
echo "  input-dir:   $INPUT_DIR"
echo "  output-dir:  $OUTPUT_DIR"
echo "  limit:       $LIMIT"
echo "  prefix-from: ${PREFIX_FROM:-<none>}"
echo "  prefix-to:   ${PREFIX_TO:-<none>}"
echo "  phase:       ${PHASE:-all}"
echo ""

cd "$REPO_ROOT"

# ── Phase 1 – Generate manifest ─────────────────────────────────────────
if [[ -z "$PHASE" || "$PHASE" == "1" ]]; then
    echo "--- Phase 1: Generating transfer manifest ---"

    MANIFEST_ARGS=(
        --database refseq
        --scan-store
        --output-manifest "$INPUT_DIR/transfer_manifest.txt"
        --output-removed "$INPUT_DIR/removed_manifest.txt"
        --output-summary "$INPUT_DIR/diff_summary.json"
        --no-upload
    )
    [[ -n "$PREFIX_FROM" ]] && MANIFEST_ARGS+=(--prefix-from "$PREFIX_FROM")
    [[ -n "$PREFIX_TO" ]]   && MANIFEST_ARGS+=(--prefix-to "$PREFIX_TO")

    uv run python scripts/ncbi/generate_transfer_manifest.py "${MANIFEST_ARGS[@]}"

    MANIFEST_LINES=$(grep -c '.' "$INPUT_DIR/transfer_manifest.txt" 2>/dev/null || echo 0)
    echo "  Manifest contains $MANIFEST_LINES assembly paths."

    if [[ "$MANIFEST_LINES" -eq 0 ]]; then
        echo "  Nothing to transfer (empty manifest). Exiting."
        exit 0
    fi

    # Apply limit by truncating manifest
    if [[ "$LIMIT" -gt 0 ]] && [[ "$MANIFEST_LINES" -gt "$LIMIT" ]]; then
        head -n "$LIMIT" "$INPUT_DIR/transfer_manifest.txt" > "$INPUT_DIR/transfer_manifest.txt.tmp"
        mv "$INPUT_DIR/transfer_manifest.txt.tmp" "$INPUT_DIR/transfer_manifest.txt"
        echo "  Truncated manifest to $LIMIT entries for testing."
    fi

    echo "  Manifest contents:"
    cat "$INPUT_DIR/transfer_manifest.txt" | sed 's/^/    /'
    echo ""
fi

# ── Phase 2 – Container download (simulates CTS) ────────────────────────
if [[ -z "$PHASE" || "$PHASE" == "2" ]]; then
    if [[ ! -f "$INPUT_DIR/transfer_manifest.txt" ]]; then
        echo "ERROR: No manifest found at $INPUT_DIR/transfer_manifest.txt"
        echo "Run Phase 1 first (--phase 1) or place a manifest there manually."
        exit 1
    fi

    echo "--- Phase 2: Container download (simulating CTS) ---"
    echo "  Building container image..."
    docker build -t "$DOWNLOADER_IMAGE" "$REPO_ROOT" -q >/dev/null

    echo "  Running container with mounted input/output..."
    docker run --rm \
        -v "$INPUT_DIR:/job_input_dir:ro" \
        -v "$OUTPUT_DIR:/job_output_dir" \
        "$DOWNLOADER_IMAGE" \
        --manifest /job_input_dir/transfer_manifest.txt \
        --output-dir /job_output_dir/ \
        --threads 2

    echo "  Container finished. Checking output..."
    FILE_COUNT=$(find "$OUTPUT_DIR" -type f ! -name '*.md5' ! -name 'download_report.json' | wc -l)
    MD5_COUNT=$(find "$OUTPUT_DIR" -name '*.md5' | wc -l)
    echo "  Data files downloaded: $FILE_COUNT"
    echo "  MD5 sidecars:         $MD5_COUNT"

    if [[ -f "$OUTPUT_DIR/download_report.json" ]]; then
        echo "  Download report:"
        python3 -c "
import json, sys
r = json.load(open('$OUTPUT_DIR/download_report.json'))
print(f'    Attempted: {r[\"total_attempted\"]}')
print(f'    Succeeded: {r[\"succeeded\"]}')
print(f'    Failed:    {r[\"failed\"]}')
"
    fi
    echo ""
fi

# ── Phase 3 – Promote to lakehouse ──────────────────────────────────────
if [[ -z "$PHASE" || "$PHASE" == "3" ]]; then
    echo "--- Phase 3: Promoting to Lakehouse ---"

    PROMOTE_ARGS=(--staging-dir "$OUTPUT_DIR")
    if [[ -f "$INPUT_DIR/removed_manifest.txt" ]]; then
        PROMOTE_ARGS+=(--removed-manifest "$INPUT_DIR/removed_manifest.txt")
    fi

    uv run python scripts/ncbi/promote_staged_files.py "${PROMOTE_ARGS[@]}"

    # Verify objects landed in MinIO
    echo "  Verifying objects in MinIO..."
    uv run python -c "
from kbase_transfers import MinioClient
c = MinioClient()
paginator = c.s3.get_paginator('list_objects_v2')
count = 0
for page in paginator.paginate(Bucket='$BUCKET', Prefix='${STORE_PREFIX}raw_data/'):
    for obj in page.get('Contents', []):
        count += 1
        if count <= 5:
            print(f'    {obj[\"Key\"]}')
if count > 5:
    print(f'    ... and {count - 5} more objects')
print(f'  Total objects in raw_data/: {count}')
"
    echo ""
fi

echo "=== E2E test complete ==="

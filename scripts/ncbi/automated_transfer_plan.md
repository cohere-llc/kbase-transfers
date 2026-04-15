# NCBI Automated Transfer Plan

Plan for semi-automated, scheduled transfers of NCBI RefSeq/GenBank genome assemblies
using the existing `download_genomes.py` script, the CTS (CDM Task Service), and the
KBase Lakehouse MinIO store.

## Background & Constraints

- **CTS containers cannot directly access the MinIO store.** The CTS stages input files
  from S3 into a container's **local filesystem** input directory and copies output files
  from the container's local output directory back to an S3 output path. The container
  itself never gets S3 credentials. The staging areas are regular filesystem mounts,
  **not** S3 paths.
- **CTS requires CRC64/NVME checksums** on all input files in S3. Files without this
  checksum are rejected.
- **Staging area size may be limited.** We need the ability to scope each run to a
  specific prefix range (e.g., `000`, `001`, ...) to avoid exceeding available disk.
- **NCBI publishes assembly summary files** at known FTP paths that list every current
  assembly with its accession, FTP path, version status, release date, and more.
  - RefSeq: `ftp.ncbi.nlm.nih.gov/genomes/refseq/assembly_summary_refseq.txt`
  - GenBank: `ftp.ncbi.nlm.nih.gov/genomes/genbank/assembly_summary_genbank.txt`
- **No pre-existing assembly summary was saved for the initial load.** Phase 1 must
  be able to reconstruct the "already transferred" set from objects currently in the
  MinIO store (using their stored MD5 and CRC64/NVME metadata) when no previous
  summary file exists.
- **Our script already supports two modes:** file-based (accession list) and prefix-based
  (recursive FTP scan). Both download genome files to a temp directory and upload them to
  MinIO with MD5 verification and frictionless metadata.

## Prior Work — PR #1

[PR #1](https://github.com/cohere-llc/kbase-transfers/pull/1) added a draft
containerised sync workflow. It was designed assuming direct S3 access from the
container, which we now know is not available. However, it contains substantial
reusable work:

| Component | What we can reuse |
|---|---|
| **`sync_genomes.py`** | Assembly summary download & parsing (`download_assembly_summary`, `parse_assembly_summary`, `get_latest_assembly_paths`). Core logic for building the assembly list from the TSV. |
| **`backfill_checksums.py`** | CRC64/NVME computation (`_compute_crc64nvme` using `awscrt.checksums.crc64nvme`), backfill-existing-objects logic. Useful for the promote step and for bootstrapping checksums on the initial load. |
| **`download_genomes.py` changes** | `compute_crc64nvme()` function, `checksum_algorithm='CRC64NVME'` on uploads, env-var-based bucket/prefix config. |
| **`minio_client.py` changes** | `stat_object` with `ChecksumMode='ENABLED'` returning `crc64nvme`, `upload_file` / `put_json_object` with `checksum_algorithm` param. |
| **`Dockerfile`** | Base image (`python:3.12-slim`), `uv` install from `ghcr.io/astral-sh/uv`, `bash` + `git` apt packages, project copy and `uv pip install`. |
| **`pyproject.toml`** | `boto3>=1.36.0` and `awscrt>=0.23.4` dependencies for CRC64/NVME support. |
| **GH Actions** | `docker-integration.yml` (MinIO + container smoke test), `docker-publish.yml` (GHCR publish), Python 3.12 pin, MinIO version pin. |
| **Tests** | `test_sync.py` (assembly summary parsing unit tests), `test_cts_integration.py` (CRC64/NVME round-trip), `test_backfill_checksums.py` (backfill integration). |

**What changes:** The container no longer talks to S3 directly. `sync_genomes.py`'s
sync loop will be adapted into `container_download.py`, which writes to the local
filesystem instead of MinIO. The S3 upload and checksum-comparison logic moves to
Phase 3 (`promote_staged_files.py`).

## High-Level Workflow (3 Phases)

```
┌──────────────────────────────────────────────────────────────────────┐
│  PHASE 1 — Diff Generation  (manual / cron on laptop or JupyterLab)│
│                                                                      │
│  1. Download latest assembly_summary_refseq.txt from NCBI FTP        │
│  2. Compare against previous version in MinIO (or, if none exists,   │
│     reconstruct the "known" set from objects in the store)           │
│  3. Produce a transfer manifest: new + updated accessions            │
│     (filterable by prefix range to control staging size)             │
│  4. Upload manifest to S3 as CTS input; archive assembly summary     │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│  PHASE 2 — Container Download  (CTS job, later scheduled via cron)   │
│                                                                      │
│  Container reads the manifest from its LOCAL input directory          │
│  (filesystem mount, not S3), downloads files from NCBI FTP, computes │
│  CRC64/NVME checksums, and writes everything to its LOCAL output     │
│  directory. CTS moves outputs back to S3.                            │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│  PHASE 3 — Promote to Lakehouse  (manual script)                     │
│                                                                      │
│  Copy staged files from the CTS S3 output prefix to the final        │
│  Lakehouse path with CRC64/NVME checksums. Archive replaced/         │
│  suppressed assemblies. Update the stored assembly summary.          │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Diff Generation Script

**New script:** `scripts/ncbi/generate_transfer_manifest.py`

### Inputs
- NCBI FTP (assembly summary file)
- MinIO: previously stored assembly summary file at
  `cdm-lake:tenant-general-warehouse/kbase/datasets/ncbi/metadata/assembly_summary_refseq.txt.gz`
- **Fallback (no previous summary):** MinIO object listing under the `raw_data/`
  prefix — extract accessions from existing object paths and use their stored
  MD5/CRC64/NVME metadata to determine what's already transferred.

### Logic
1. Download `assembly_summary_refseq.txt` (and/or genbank) from NCBI FTP.
   - Reuse `download_assembly_summary()` and `parse_assembly_summary()` from
     PR #1's `sync_genomes.py`.
2. Load the previously stored version from MinIO (if it exists).
   - If no previous summary exists (first run after initial load), scan the
     MinIO store under `raw_data/` to build the set of already-transferred
     accessions. For each assembly dir found, check that the expected files
     exist and their MD5 metadata matches NCBI's `md5checksums.txt`.
     This produces a synthetic "previous" set.
3. Parse both as TSV (skip `#` header lines). Key columns:
   - Col 1: `assembly_accession` (e.g., `GCF_000005845.2`)
   - Col 11: `version_status` — only transfer rows where this is `latest`
   - Col 15: `seq_rel_date` — sequence release date, for detecting updates
   - Col 20: `ftp_path` — direct FTP path to the assembly directory
4. Compute the diff:
   - **New assemblies:** accession present in new file but not in previous/store
   - **Updated assemblies:** accession present in both but `seq_rel_date` or
     assembly version changed
   - **Replaced/suppressed assemblies:** accession in previous but status changed
     to `replaced`/`suppressed` — include in the manifest for archival action
5. **Filter by prefix range:** `--prefix-from` / `--prefix-to` to limit the
   manifest to accessions whose 3-digit prefix falls in range (e.g., `000`–`003`).
   This keeps any single CTS job's output within staging disk limits.
6. Write outputs:
   - `transfer_manifest_YYYYMMDD.txt` — one FTP assembly path per line, compatible
     with our script's assembly-path mode
   - `removed_manifest_YYYYMMDD.txt` — replaced/suppressed accessions to archive
   - `diff_summary_YYYYMMDD.json` — structured summary: counts of
     new/updated/removed, NCBI release version, prefix range, timestamps
   - Upload the new assembly summary file to MinIO at a versioned path (gzipped):
     `ncbi/metadata/assembly_summary_refseq_YYYYMMDD.txt.gz`

### CLI
```bash
python generate_transfer_manifest.py \
    --database refseq \
    --output-manifest transfer_manifest_20260415.txt \
    --output-removed removed_manifest_20260415.txt \
    --output-summary diff_summary_20260415.json \
    --prefix-from 000 --prefix-to 003   # optional: limit to prefix range
```

### Fallback mode (no previous summary)
```bash
python generate_transfer_manifest.py \
    --database refseq \
    --scan-store \                       # reconstruct "previous" from MinIO objects
    --output-manifest transfer_manifest_20260415.txt \
    --prefix-from 000 --prefix-to 003
```

### Can also be used from a notebook
```python
from generate_transfer_manifest import generate_manifest
manifest_path, summary = generate_manifest(
    database="refseq",
    prefix_from="000", prefix_to="003",
)
print(f"Found {summary['new']} new, {summary['updated']} updated assemblies")
```

---

## Phase 2: CTS Container Job

### Container Image

**Dockerfile** (adapted from PR #1, updated for the no-S3-access model):

```dockerfile
FROM python:3.12-slim

# CTS requires /bin/bash; git needed for dtspy dependency
RUN apt-get update && apt-get install -y --no-install-recommends bash git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml ./
COPY kbase_transfers/ kbase_transfers/
COPY scripts/ncbi/ scripts/ncbi/

# Install the package and dependencies
RUN uv pip install --system --no-cache .

ENTRYPOINT ["python", "scripts/ncbi/container_download.py"]
```

The container's entrypoint script will:
1. Read the transfer manifest from the **local** input directory
   (CTS mounts this from S3 to a filesystem path, e.g., `/job_input_dir/`)
2. For each assembly path in the manifest, download the filtered files from
   NCBI FTP to the **local** output directory (e.g., `/job_output_dir/`)
   using the same directory structure (`raw_data/GCF/000/005/845/...`)
3. Verify MD5 checksums against NCBI's `md5checksums.txt`
4. Compute CRC64/NVME checksums for each downloaded file and save them in a
   sidecar file (`<filename>.crc64nvme`) — these will be used during the
   Phase 3 promote step to attach checksums to S3 objects
5. Generate a frictionless `datapackage.json` descriptor for each assembly
6. Write a download report to the output directory

**Key difference from current script:** The container does NOT upload to MinIO.
It only writes to the local filesystem. CTS handles moving outputs to S3.

### New script: `scripts/ncbi/container_download.py`

Adapted from PR #1's `sync_genomes.py`, but with S3 calls removed:
- Reads the manifest from a local path (default: `/job_input_dir/transfer_manifest.txt`)
- Reuses existing `download_genome_files()` logic, refactored to separate
  FTP download from MinIO upload — extract a `download_to_local()` function
- Computes CRC64/NVME checksums using `awscrt.checksums.crc64nvme` (from PR #1)
- Writes outputs to a local directory (default: `/job_output_dir/`)

```bash
python container_download.py \
    --manifest /job_input_dir/transfer_manifest.txt \
    --output-dir /job_output_dir/ \
    --threads 4
```

### CTS Job Submission

```python
import requests

res = requests.post(
    "https://berdl.kbase.us/apis/cts/jobs/",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "cluster": "kbase-htcondor",   # or whichever site is available
        "image": "ghcr.io/cohere-llc/kbase-ncbi-transfer:<tag>",
        "params": {
            "input_mount_point": "/job_input_dir",
            "output_mount_point": "/job_output_dir",
            "args": [
                "--manifest", "/job_input_dir/transfer_manifest.txt",
                "--output-dir", "/job_output_dir/",
                "--threads", "4",
            ],
        },
        "input_files": [
            "cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/staging/transfer_manifest.txt"
        ],
        "output_dir": "cdm-lake/tenant-general-warehouse/kbase/datasets/ncbi/staging/output",
        "runtime": "PT4H",
        "cpus": 4,
        "num_containers": 1,
        "memory": "8GB",
    }
)
```

### CTS Image Registration (admin, one-time)

An admin must approve the image before it can be used:
```
POST /admin/images/ghcr.io/cohere-llc/kbase-ncbi-transfer:<tag>
```

---

## Phase 3: Promote to Lakehouse

**New script:** `scripts/ncbi/promote_staged_files.py`

After the CTS job completes, the downloaded files will be in the CTS output prefix
in S3. This script:

1. Lists all files under the staging output prefix in S3
2. For each assembly directory:
   a. If the assembly already exists in the store and is being **updated**,
      archive the old version first (see "Archiving replaced assemblies" below)
   b. Copies files to the final Lakehouse path
      (`cdm-lake:tenant-general-warehouse/kbase/datasets/ncbi/raw_data/...`)
   c. Reads the CRC64/NVME checksums from sidecar files produced by the
      container and attaches them during upload (via boto3 `ChecksumAlgorithm`
      / `ChecksumCRC64NVME` params, as demonstrated in PR #1's backfill script)
   d. Copies the frictionless `datapackage.json` to the metadata path
3. Processes the removed manifest (`removed_manifest.txt`):
   - Archives replaced/suppressed assemblies (see below)
4. Updates the stored assembly summary reference file
5. Cleans up the staging prefix
6. Writes a promotion report

### Archiving replaced/suppressed assemblies

When an assembly in the store is being replaced by a new version, or NCBI marks
it as replaced/suppressed:

1. Move the existing files from `raw_data/GCF/...` to
   `archive/<ncbi_release>/GCF/...` where `<ncbi_release>` is the NCBI RefSeq
   release version (e.g., `229`) that was current when the record was last
   known to be valid.
2. Tag the archived objects with metadata:
   `ncbi_last_release=229`, `archive_reason=replaced|suppressed`,
   `archive_date=2026-04-15`
3. This preserves the old data while keeping the `raw_data/` prefix clean
   for the current version.

### CLI
```bash
python promote_staged_files.py \
    --staging-prefix tenant-general-warehouse/kbase/datasets/ncbi/staging/output/ \
    --removed-manifest removed_manifest_20260415.txt \
    --ncbi-release 229 \
    --dry-run   # preview what would be promoted/archived
```

---

## S3 Path Layout

```
cdm-lake/
└── tenant-general-warehouse/kbase/datasets/ncbi/
    ├── metadata/
    │   ├── assembly_summary_refseq.txt.gz            # current reference (compressed)
    │   ├── assembly_summary_refseq_20260315.txt.gz    # versioned archive (compressed)
    │   ├── assembly_summary_refseq_20260415.txt.gz
    │   ├── GCF_000005845.2_ASM584v2_datapackage.json  # per-assembly descriptors
    │   └── ...
    ├── raw_data/                                       # current/latest assemblies
    │   └── GCF/000/005/845/GCF_000005845.2_ASM584v2/
    │       ├── GCF_000005845.2_ASM584v2_genomic.fna.gz
    │       ├── GCF_000005845.2_ASM584v2_genomic.gff.gz
    │       ├── GCF_000005845.2_ASM584v2_protein.faa.gz
    │       ├── md5checksums.txt
    │       └── ...
    ├── archive/                                        # replaced/suppressed versions
    │   └── 229/GCF/000/005/845/GCF_000005845.1_ASM584v1/
    │       └── ...                                     # tagged with ncbi_last_release
    └── staging/                                        # CTS I/O area (in S3)
        └── transfer_manifest.txt                       # input to container
        (CTS output goes to a separate output_dir prefix)
```

**Note on staging:** The `staging/` prefix in S3 holds the manifest file that CTS
copies to the container's **local filesystem** input directory. The container writes
to a **local filesystem** output directory, and CTS copies those files back to the
S3 output prefix. The container never interacts with S3 directly.

---

## Implementation Tasks

### 1. Merge PR #1 CRC64/NVME and parsing foundations
- [ ] Cherry-pick from PR #1: `minio_client.py` changes (CRC64/NVME `stat_object`,
      `upload_file` with `checksum_algorithm`, `put_json_object` with checksum)
- [ ] Cherry-pick: `compute_crc64nvme()` in `download_genomes.py`
- [ ] Cherry-pick: `pyproject.toml` dep updates (`boto3>=1.36.0`, `awscrt>=0.23.4`)
- [ ] Cherry-pick: `backfill_checksums.py` (useful for initial load and promote)
- [ ] Cherry-pick: assembly summary parsing from `sync_genomes.py`
      (`download_assembly_summary`, `parse_assembly_summary`, `get_latest_assembly_paths`)
- [ ] Cherry-pick: unit tests (`test_sync.py`, `test_cts_integration.py`,
      `test_backfill_checksums.py`)

### 2. `generate_transfer_manifest.py` (Phase 1)
- [ ] Download and parse NCBI assembly summary TSV (reuse PR #1 parsing)
- [ ] Load previous assembly summary from MinIO (gzipped)
- [ ] Fallback: scan MinIO store to reconstruct "previous" set from existing objects
- [ ] Compute diff (new/updated/replaced/suppressed)
- [ ] Prefix range filtering (`--prefix-from`, `--prefix-to`)
- [ ] Write transfer manifest and removed manifest files
- [ ] Write JSON summary
- [ ] Upload new assembly summary to MinIO (versioned, gzipped)
- [ ] CLI with `--database`, `--scan-store`, `--prefix-from/to`, etc.
- [ ] Unit tests with mock FTP and MinIO data

### 3. `container_download.py` (Phase 2)
- [ ] Refactor download logic from `download_genomes.py` to separate FTP download
      from MinIO upload — extract a `download_to_local()` function
- [ ] Read manifest, download each assembly to local output dir
- [ ] Preserve existing directory structure (`raw_data/GCF/...`)
- [ ] Compute and save CRC64/NVME checksums as sidecar files
- [ ] Generate frictionless descriptors alongside downloaded files
- [ ] Write download report JSON
- [ ] Unit tests

### 4. Dockerfile & CI
- [ ] Adapt PR #1 Dockerfile for the no-S3-access model
- [ ] Adapt PR #1 GH Actions (`docker-integration.yml`, `docker-publish.yml`)
- [ ] Test locally with mock CTS directory structure
- [ ] Push to `ghcr.io/cohere-llc/kbase-ncbi-transfer`

### 5. CTS Integration
- [ ] Register the image with CTS admin
- [ ] Write a submission helper script or notebook cell
- [ ] Test with a small manifest (2-3 assemblies)
- [ ] Document job monitoring via CTS API

### 6. `promote_staged_files.py` (Phase 3)
- [ ] List staged files from CTS output prefix
- [ ] Copy to final Lakehouse paths, attaching CRC64/NVME checksums from sidecars
      (reuse `awscrt` CRC64/NVME approach from PR #1's backfill)
- [ ] Move descriptors to metadata path
- [ ] Archive replaced/suppressed assemblies with version tagging
- [ ] Update the reference assembly summary (gzipped)
- [ ] Cleanup staging area
- [ ] `--dry-run` mode
- [ ] Unit tests

### 7. End-to-End Testing
- [ ] Run full pipeline with local MinIO + small test set
- [ ] Test `--scan-store` fallback with pre-populated store
- [ ] Test prefix range filtering
- [ ] Test archive/replace flow
- [ ] Run against real MinIO with `--limit 5`
- [ ] Verify checksums, metadata, and final paths

---

## Resolved Questions

1. **Container networking:** Confirmed — CTS containers have outbound internet
   access. Similar transfer scripts have already been used with CTS.

2. **CTS output checksums:** CTS does NOT automatically add CRC64/NVME checksums
   to output files. The container script will compute CRC64/NVME checksums
   during download and save them as sidecar files. The Phase 3 promote script
   reads these and attaches them when uploading to the final Lakehouse path.

3. **CTS job scheduling:** CTS only has on-demand job submission. We will set up
   an external cron job to trigger Phase 2 after the end-to-end pipeline is
   working and tested.

4. **S3 access from CTS containers:** Not needed. The diff is computed in Phase 1
   (outside the container) and the promote happens in Phase 3 (also outside the
   container). The container only downloads from NCBI FTP to its local output dir.

5. **CTS staging areas:** These are **local filesystem mounts**, not S3 paths.
   CTS copies S3 input files to the container's local input directory, and copies
   the container's local output directory back to S3 after the job completes.

6. **No pre-existing assembly summary:** Phase 1 supports a `--scan-store` fallback
   that reconstructs the "already transferred" set by listing MinIO objects and
   checking their metadata (MD5, CRC64/NVME). This handles the initial load case.

7. **Staging size limits:** Phase 1 supports `--prefix-from` / `--prefix-to`
   filtering so each CTS job only covers a manageable subset of accessions.

8. **Assembly summary storage:** Versioned copies will be stored gzipped to reduce
   the ~1 GB uncompressed size.

9. **Replaced/suppressed assemblies:** Preserved in the MinIO store under an
   `archive/<ncbi_release>/` prefix, tagged with the last NCBI release version
   where they were current.

## Open Questions

(None at this time — all raised questions have been resolved.)

---

## Coordination Notes

- **(AJ)** Register the container image as a CTS admin
- **(Matt)** Cherry-pick reusable code from PR #1
- **(Matt)** Implement Phase 1 script (`generate_transfer_manifest.py`)
- **(Matt)** Refactor `download_genomes.py` and create `container_download.py`
- **(Matt)** Adapt Dockerfile and CI from PR #1
- **(Matt)** Implement Phase 3 script (`promote_staged_files.py`)
- **(Matt)** End-to-end test with small dataset
- **(Matt + AJ)** Set up cron scheduling after pipeline is proven

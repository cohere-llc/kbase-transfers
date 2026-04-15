# NCBI File Transfer Script

The python script in this folder downloads files from `ftp.ncbi.nlm.nih.gov`. The script
takes argument:
- a path to a text file containing a list of genome records to download

Each entry in the text file is in the form:
```
{PREFIX}_{TYPE}_{ID}.{RECORD}
```
- `PREFIX`: Either `GB` (GenBank) or `RS` (RefSeq). This is ignored in the query
- `DATABASE`: Either `GCA` (Seems to correspond to `GB` records?) or `GCF` (`RS` records?)
- `ID`: Nine digit ID, split into three, 3-digit parts for the query: `PART1`, `PART2`, `PART3`
- `RECORD`: Integer used to identify specific subfolders in the `ID` record. Starts at 1, goes up to the number of subfolders

The path to a specific folder's files is:
```
ftp://ftp.ncbi.nlm.nih.gov/genomes/all/{DATABASE}/{PART1}/{PART2}/{PART3}/{DATABASE}_{ID}.{RECORD}_SomeLabelText
```
The text after `{RECORD}_` describes the record in some way, but only the integer record index is used in the query (this assumes one sub-folder per record id, which seems to be the case).

Here is an example list:
```
GB_GCA_000195005.1
GB_GCA_000408925.1
GB_GCA_000410835.1
GB_GCA_000452465.2
GB_GCA_000682095.1
RS_GCF_000006825.1
RS_GCF_000007865.1
RS_GCF_000008205.1
```

Only a subset of the files in each record subfolder are downloaded, following this logic:

| Filter | Database | Format | Description |
|--------|----------|--------|-------------|
| `*_gene_ontology.gaf.gz` | `R` | GO Annotation File (GAF) | Gene Ontology (GO) annotation of the annotated genes. |
| `*_genomic.fna.gz` | `D/G/R` | FASTA | Genomic sequence(s) in the assembly. Repetitive sequences in eukaryotes are masked to lower-case. |
| `*_genomic.gff.gz` | `D/G/R` | GFF3 | Annotation of the genomic sequence(s). |
| `*_protein.faa.gz` | `D/G/R` | FASTA | Sequences of accessioned protein products annotated on the genome assembly. |
| `*_ani_contam_ranges.tsv` | `G/R` | Tab-delimited text | Reports potentially contaminated regions in the assembly identified based on Average Nucleotide Identity (ANI). |
| `assembly_*.txt` | `G/R` | Tab-delimited text | Assembly reports and statistics |
| `*_normalized_gene_expression_counts.txt.gz` | `R` | Tab-delimited text | Reports normalized counts (TPM) of RNA-seq reads mapped to each gene. |

* `D`: Datasets available on NCBI Datasets site
* `G`: GenBank
* `R`: RefSeq

The script uses a temporary local folder for staging files prior to uploading them to a MinIO instance. The MinIO client is provided by the shared `kbase_transfers` package. 

By default, the script expects a running MinIO instance set up for testing (see [Testing with MinIO](#testing-with-minio) or the [main README](../../README.md#testing-with-containerized-minio)). If the following environment variables are set, they will be used as credentials (making it usable in the lakehouse for real transfers):
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_ENDPOINT_URL`

The script expects the bucket `cdm-lake` to exist and the path `tenant-general-warehouse/kbase/datasets/ncbi/` to contain at
least one file or subfolder.

## Install Dependencies and Run Tests

### Install uv

This project uses [uv](https://docs.astral.sh/uv/) for fast Python package management:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install the kbase_transfers package

From the repository root:
```bash
# Install all dependencies (creates .venv automatically)
uv sync
```

This installs the shared `kbase_transfers` package in editable mode, making the MinIO client available to all scripts.

### Testing with MinIO

Set up a local MinIO server if testing locally (requires docker or podman):

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  -d docker.io/minio/minio server /data --console-address ":9001"
```

Now, navigate to `http://localhost:9001`, log in with the user name and password (both `minioadmin`) and add
the `cdm-lake` bucket and upload a small file to `cdm-lake:tenant-general-warehouse/kbase/datasets/ncbi/`

See the [main README](../../README.md#testing-with-containerized-minio) for more details on MinIO setup.

### Run the script

The `test_list.txt` file contains 8 record set IDs and can be used to test the transfer script.

```bash
# From the repository root, install dependencies
uv sync

# Run the MinIO client tests
uv run pytest tests/test_minio_client.py -v

# Run the download script
uv run python scripts/ncbi/download_genomes.py test_list.txt
```

## Example usage

### Basic Usage

```bash
# Download genomes from accession list
uv run python scripts/ncbi/download_genomes.py scripts/ncbi/list_of_accessions_x86clades.txt

# Or use the test list
uv run python scripts/ncbi/download_genomes.py scripts/ncbi/test_list.txt

# Limit to first N accessions for testing
uv run python scripts/ncbi/download_genomes.py scripts/ncbi/test_list.txt --limit 2
```

### Prefix-based Usage

```bash
# Download all genomes under a specific FTP prefix
uv run python scripts/ncbi/download_genomes.py --prefix GCF/000/001

# Download from a broad prefix and save the discovered accession list
uv run python scripts/ncbi/download_genomes.py --prefix GCF/000 --output-list assemblies.txt

# Use multiple threads for faster parallel downloads
uv run python scripts/ncbi/download_genomes.py --prefix GCF/000/001 --threads 8

# Resume an interrupted run from a specific top-level subdirectory,
# processing one subdirectory at a time to avoid building a huge list upfront
uv run python scripts/ncbi/download_genomes.py --prefix GCF --start-from 003 --threads 8

# Combine all options for a large-scale resumable run
uv run python scripts/ncbi/download_genomes.py \
  --prefix GCF \
  --start-from 003 \
  --output-list gcf_accessions.txt \
  --threads 8 \
  --limit 1000
```

### Python API Usage

`run()` can also be called directly from Python (e.g. from a notebook):

```python
from scripts.ncbi.download_genomes import run

# Download from an accession list file
run(input_file='scripts/ncbi/test_list.txt')

# Download all assemblies under a prefix, 4 threads, stop after 100
run(prefix='GCF', limit=100, threads=4)

# Resume a prefix run from subdirectory '003', saving discovered accessions
run(prefix='GCF', start_from='003', output_list='gcf_accessions.txt', threads=8)
```

### Command-Line Options

| Option | Description |
|--------|-------------|
| `input_file` | File with list of accessions, one per line (mutually exclusive with `--prefix`) |
| `--prefix PREFIX` | FTP sub-path under `/genomes/all/` to scan recursively, e.g. `GCF` or `GCF/000/001` (mutually exclusive with `input_file`) |
| `--start-from SUBDIR` | When using `--prefix`, skip top-level subdirectories that sort before `SUBDIR` (e.g. `003`). Useful for resuming an interrupted run. |
| `--output-list FILE` | File to write discovered accession IDs incrementally (only valid with `--prefix`) |
| `--ftp-host HOST` | FTP hostname (default: `ftp.ncbi.nlm.nih.gov`) |
| `--threads N` | Number of parallel download threads (default: `1`) |
| `--limit N` | Stop after N assemblies have been attempted (useful for smoke-testing) |

**Note:** Either provide an `input_file` or use `--prefix`, but not both.

## Output Structure

```
python3 download_genomes.py example_list.txt
```

The contents of my folder`cdm-lake:tenant-general-warehouse/kbase/datasets/ncbi/raw_data/` would look like this:
```
|- my-folder/
   |- GCA/000/195/005/GCA_000195005.1_foobar/
   |- GCA/000/408/925/GCA_000408925.1_barbaz/
   |- GCA/000/410/835/GCA_000410835.1_bazqux/
   |- GCA/000/425/465/GCA_000452465.2_quxquux/
   |- GCA/000/682/095/GCA_000682095.1_quuxcorge/
   |- GCF/000/006/825/GCF_000006825.1_corge/
   |- GCF/000/007/865/GCF_000007865.1_quux/
   |- GCF/000/008/205/GCF_000008205.1_qux/
   ```

---

## Semi-Automated Transfer Pipeline

In addition to the manual `download_genomes.py` script above, this folder contains a
three-phase pipeline for **incremental**, **checksum-verified** transfers of NCBI
assembly data into the Lakehouse object store.

| Phase | Script | Runs where | Purpose |
|-------|--------|------------|---------|
| 1 | `generate_transfer_manifest.py` | JupyterLab | Diff the current NCBI assembly summary against the previous snapshot and produce a manifest of assemblies to download. |
| 2 | `container_download.py` | CTS container (prod) or local Docker (test) | Download assemblies from NCBI FTP to a local filesystem, verify MD5 checksums, write MD5 sidecar files. CTS computes CRC64/NVME on upload. |
| 3 | `promote_staged_files.py` | JupyterLab | Copy downloaded files from the staging area to the final Lakehouse path, archive replaced/suppressed assemblies. |

### How it works

1. **Phase 1** downloads the latest `assembly_summary_refseq.txt` from NCBI,
   compares it to the previous snapshot stored in MinIO, and writes:
   - `transfer_manifest_<date>.txt` — FTP paths of assemblies to download (new + updated)
   - `removed_manifest_<date>.txt` — accessions that have been replaced or suppressed
   - `diff_summary_<date>.json` — counts and metadata about the diff

2. **Phase 2** reads the transfer manifest and downloads each assembly from NCBI
   FTP.  For every downloaded file it verifies the NCBI MD5 checksum and writes
   a `.md5` sidecar.  In production this runs inside a CTS container (CTS
   computes CRC64/NVME checksums when uploading output to S3); locally you
   can run the same container image with Docker volume mounts.

3. **Phase 3** reads the downloaded files (from a local directory or an S3
   staging prefix) and uploads them to the final Lakehouse paths in MinIO
   with CRC64/NVME checksums and NCBI MD5 metadata.  Replaced/suppressed
   assemblies are moved to an `archive/<release>/` prefix with version-tag
   metadata.

### Local End-to-End Testing

The script `test_e2e_local.sh` runs the three phases locally against a
user-managed MinIO instance, using a Docker container for Phase 2
(simulating what CTS does in production).  Because you manage the MinIO
container and staging directories yourself, you can inspect the S3 store
between phases, look at intermediate files, and rerun phases to test
retransfers over existing records.

**Prerequisites:** Docker (or Podman symlinked to `docker`) and `uv`.

#### 1. Start MinIO and create the bucket

```bash
# Start MinIO on port 9100 (avoids clashing with a dev instance on 9000)
docker run -d --name test-minio -p 9100:9000 -p 9101:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  docker.io/minio/minio:RELEASE.2025-02-28T09-55-16Z server /data --console-address ":9001"

# Export credentials (keep this shell open, or add to your .env)
export MINIO_ENDPOINT_URL=http://localhost:9100
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin

# Create the bucket and seed the expected prefix
uv run python -c "
from kbase_transfers import MinioClient
c = MinioClient()
c.s3.create_bucket(Bucket='cdm-lake')
c.s3.put_object(Bucket='cdm-lake',
    Key='tenant-general-warehouse/kbase/datasets/ncbi/.keep', Body=b'')
print('Bucket created and seeded.')
"
```

You can browse the store at any time via the MinIO console at
`http://localhost:9101` (user/pass: `minioadmin`).

#### 2. Create staging directories

```bash
mkdir -p /tmp/ncbi_input /tmp/ncbi_output
```

These persist across runs so you can inspect manifests and downloaded
files between phases.

#### 3. Run the pipeline

```bash
# Run all three phases
./scripts/ncbi/test_e2e_local.sh \
    --input-dir /tmp/ncbi_input --output-dir /tmp/ncbi_output \
    --limit 2 --prefix-from 000 --prefix-to 000

# Or run individual phases (inspect the store / staging dirs between them)
./scripts/ncbi/test_e2e_local.sh --input-dir /tmp/ncbi_input --output-dir /tmp/ncbi_output --phase 1
# ... inspect /tmp/ncbi_input/transfer_manifest.txt, diff_summary.json ...
./scripts/ncbi/test_e2e_local.sh --input-dir /tmp/ncbi_input --output-dir /tmp/ncbi_output --phase 2
# ... inspect /tmp/ncbi_output/raw_data/*, download_report.json ...
./scripts/ncbi/test_e2e_local.sh --input-dir /tmp/ncbi_input --output-dir /tmp/ncbi_output --phase 3
# ... browse MinIO console to verify objects ...

# Retransfer test: run all phases again — Phase 1 will see existing records
# in the store and produce only a diff of what's new/changed.
./scripts/ncbi/test_e2e_local.sh \
    --input-dir /tmp/ncbi_input --output-dir /tmp/ncbi_output \
    --limit 2 --prefix-from 000 --prefix-to 000
```

#### 4. Cleanup (when done)

```bash
docker rm -f test-minio
rm -rf /tmp/ncbi_input /tmp/ncbi_output
```

### Running the Unit Tests

```bash
# From the repository root
uv sync
uv run pytest tests/test_sync.py -v
```

---

## Production Setup

### Prerequisites

- MinIO (or S3-compatible store) with the `cdm-lake` bucket, accessible from
  the Lakehouse JupyterLab environment
- Container image published to a registry accessible by CTS:
  ```bash
  docker build -t ghcr.io/cohere-llc/kbase-ncbi-transfer:latest .
  docker push ghcr.io/cohere-llc/kbase-ncbi-transfer:latest
  ```
  (The `docker-publish.yml` GitHub Actions workflow does this automatically on
  pushes to `main` and version tags.)

### One-Time CTS Setup

Before the first production run, register the container image with CTS
(requires admin privileges):

```
POST /admin/images/ghcr.io/cohere-llc/kbase-ncbi-transfer:<tag>
```

### Architecture: Why CTS for Phase 2?

The Lakehouse JupyterLab environment has direct access to the S3/MinIO
store, which is ideal for Phases 1 and 3 (manifest generation and file
promotion).  However, JupyterLab does not support persistent scheduled
tasks — sessions time out and there is no cron.  CTS fills this gap:
it provides on-demand container execution on cluster hardware, with
automatic staging of inputs from S3 and outputs back to S3.

| Phase | Where it runs | Why |
|-------|---------------|-----|
| 1 — Manifest generation | JupyterLab | Needs S3 access to read previous summary and upload new one |
| 2 — NCBI download | CTS container | Long-running; benefits from cluster bandwidth; no S3 access needed (CTS stages I/O) |
| 3 — Promote to Lakehouse | JupyterLab | Needs S3 access to copy files and archive old assemblies |

For **manual runs**, all three phases can be driven interactively from a
JupyterLab terminal.  For **scheduled runs**, a cron job on a local
machine can periodically submit Phase 2 to CTS (see
[Scheduled Phase 2](#scheduled-phase-2-local-cron--cts)), but Phases 1
and 3 are always run manually from JupyterLab.

### Manually-Triggered Transfer (JupyterLab)

Run each phase from a JupyterLab terminal.  Phases 1 and 3 have direct
access to the MinIO store; Phase 2 is submitted to CTS.

#### Phase 1 — Generate the manifest

```bash
# Generate the manifest (runs in JupyterLab terminal)
python scripts/ncbi/generate_transfer_manifest.py \
    --database refseq

# Review what will be transferred
cat transfer_manifest_*.txt | wc -l
cat diff_summary_*.json | python3 -m json.tool

# Optional: limit to a prefix range for a smaller batch
python scripts/ncbi/generate_transfer_manifest.py \
    --database refseq \
    --prefix-from 000 --prefix-to 003
```

The manifest is uploaded to MinIO at:
`tenant-general-warehouse/kbase/datasets/ncbi/staging/transfer_manifest.txt`

#### Phase 2 — Submit the CTS job

From a JupyterLab notebook cell or terminal:

```python
import requests

CTS_URL = "https://berdl.kbase.us/apis/cts"
TOKEN = "..."  # your auth token

STAGING_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/staging"
IMAGE = "ghcr.io/cohere-llc/kbase-ncbi-transfer:latest"

res = requests.post(
    f"{CTS_URL}/jobs/",
    headers={"Authorization": f"Bearer {TOKEN}"},
    json={
        "cluster": "kbase-htcondor",
        "image": IMAGE,
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
            f"cdm-lake/{STAGING_PREFIX}/transfer_manifest.txt"
        ],
        "output_dir": f"cdm-lake/{STAGING_PREFIX}/output",
        "runtime": "PT4H",
        "cpus": 4,
        "num_containers": 1,
        "memory": "8GB",
    },
)
res.raise_for_status()
job_id = res.json()["id"]
print(f"CTS job submitted: {job_id}")
```

Monitor the job (re-run this cell periodically):

```python
status = requests.get(
    f"{CTS_URL}/jobs/{job_id}",
    headers={"Authorization": f"Bearer {TOKEN}"},
).json()
print(status["state"])  # CREATED, RUNNING, COMPLETE, FAILED, ...
```

When the job reaches `COMPLETE`, its output (downloaded files + MD5
sidecars + `download_report.json`) will be at the `output_dir` prefix in S3,
with CRC64/NVME checksums attached by CTS.

#### Phase 3 — Promote to the Lakehouse

Once the CTS job is complete, run from JupyterLab:

```bash
python scripts/ncbi/promote_staged_files.py \
    --staging-prefix tenant-general-warehouse/kbase/datasets/ncbi/staging/output/ \
    --removed-manifest removed_manifest_*.txt \
    --ncbi-release 229
```

Use `--dry-run` on any phase to preview without making changes:

```bash
python scripts/ncbi/generate_transfer_manifest.py --database refseq --dry-run
python scripts/ncbi/promote_staged_files.py \
    --staging-prefix tenant-general-warehouse/kbase/datasets/ncbi/staging/output/ --dry-run
```

### Scheduled Phase 2 (Local Cron + CTS)

Since JupyterLab sessions cannot run persistent cron jobs, Phase 2 can be
scheduled from a local machine.  The cron job submits the CTS container
job and (optionally) polls until it finishes.  Phases 1 and 3 are still
run manually from JupyterLab before and after.

Typical workflow:

1. **JupyterLab** — Run Phase 1 to generate and upload the manifest.
2. **Local cron** — Picks up the manifest from S3 and submits Phase 2 to
   CTS on a schedule (or you trigger it manually).
3. **JupyterLab** — Once Phase 2 completes, run Phase 3 to promote files.

Create a script on the cron machine, e.g. `submit_cts_download.sh`:

```bash
#!/usr/bin/env bash
#
# Submit the NCBI download container job to CTS.
#
# Expects that Phase 1 has already been run (manifest uploaded to S3).
# After this job completes, run Phase 3 from JupyterLab.
#
# Schedule with cron on a machine that has:
#   - Network access to the CTS API
#   - The `requests` Python package installed
#
set -euo pipefail

CTS_URL="https://berdl.kbase.us/apis/cts"
CTS_TOKEN="..."                          # or read from a secrets file
CTS_IMAGE="ghcr.io/cohere-llc/kbase-ncbi-transfer:latest"
CTS_CLUSTER="kbase-htcondor"
STAGING_PREFIX="tenant-general-warehouse/kbase/datasets/ncbi/staging"

LOG_DIR="/var/log/ncbi-sync"
mkdir -p "$LOG_DIR"

echo "[$(date -u)] Submitting CTS download job..."
JOB_ID=$(python3 -c "
import requests, json, sys

res = requests.post(
    '${CTS_URL}/jobs/',
    headers={'Authorization': 'Bearer ${CTS_TOKEN}'},
    json={
        'cluster': '${CTS_CLUSTER}',
        'image': '${CTS_IMAGE}',
        'params': {
            'input_mount_point': '/job_input_dir',
            'output_mount_point': '/job_output_dir',
            'args': [
                '--manifest', '/job_input_dir/transfer_manifest.txt',
                '--output-dir', '/job_output_dir/',
                '--threads', '4',
            ],
        },
        'input_files': [
            'cdm-lake/${STAGING_PREFIX}/transfer_manifest.txt'
        ],
        'output_dir': 'cdm-lake/${STAGING_PREFIX}/output',
        'runtime': 'PT4H',
        'cpus': 4,
        'num_containers': 1,
        'memory': '8GB',
    },
)
res.raise_for_status()
print(res.json()['id'])
")
echo "[$(date -u)] CTS job submitted: $JOB_ID" | tee -a "$LOG_DIR/cts_jobs.log"

# Optional: poll for completion (remove this block if you prefer to
# check job status manually from JupyterLab)
echo "[$(date -u)] Polling for completion..."
while true; do
    STATE=$(python3 -c "
import requests
res = requests.get(
    '${CTS_URL}/jobs/$JOB_ID',
    headers={'Authorization': 'Bearer ${CTS_TOKEN}'},
)
print(res.json()['state'])
" 2>/dev/null || echo "UNKNOWN")

    case "\$STATE" in
        COMPLETE)
            echo "[$(date -u)] CTS job completed.  Run Phase 3 from JupyterLab."
            break
            ;;
        FAILED|ERROR)
            echo "[$(date -u)] ERROR: CTS job \$STATE. Check CTS logs."
            exit 1
            ;;
        *)
            echo "[$(date -u)]   Job state: \$STATE"
            sleep 300   # poll every 5 minutes
            ;;
    esac
done
```

Schedule with cron (e.g. weekly on Sundays at 02:00 UTC):

```bash
crontab -e
# Add:
0 2 * * 0 /opt/kbase-transfers/submit_cts_download.sh >> /var/log/ncbi-sync/cron.log 2>&1
```

Or with a systemd timer:

```ini
# /etc/systemd/system/ncbi-sync.service
[Unit]
Description=NCBI Assembly Download (CTS Phase 2)

[Service]
Type=oneshot
ExecStart=/opt/kbase-transfers/submit_cts_download.sh
User=kbase
Environment=PATH=/usr/local/bin:/usr/bin

# /etc/systemd/system/ncbi-sync.timer
[Unit]
Description=Weekly NCBI Download Submission

[Timer]
OnCalendar=Sun *-*-* 02:00:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ncbi-sync.timer
```

> **Note:** The cron machine only needs network access to the CTS API and
> Python with the `requests` package.  It does not need MinIO credentials
> or Docker — all S3 interaction is handled by CTS and JupyterLab.

### Backfilling CRC64/NVME Checksums

If the store already has objects that were uploaded without CRC64/NVME checksums
(e.g. from the original `download_genomes.py`), use the backfill script:

```bash
# Dry run first
uv run python scripts/ncbi/backfill_checksums.py \
    --prefix tenant-general-warehouse/kbase/datasets/ncbi/raw_data/ \
    --dry-run

# Backfill (re-uploads each object with CRC64/NVME)
uv run python scripts/ncbi/backfill_checksums.py \
    --prefix tenant-general-warehouse/kbase/datasets/ncbi/raw_data/ \
    --limit 100
```

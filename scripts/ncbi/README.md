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

### Install the kbase_transfers package

From the repository root:
```bash
pip install -e .
```

This installs the shared `kbase_transfers` package in editable mode, making the MinIO client available to all scripts.

### Testing with MinIO

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
# From the repository root, install the package
pip install -e .

# Run the MinIO client tests
python -m pytest tests/test_minio_client.py -v

# Run the download script
cd scripts/ncbi
python download_genomes.py test_list.txt
```

## Example usage
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

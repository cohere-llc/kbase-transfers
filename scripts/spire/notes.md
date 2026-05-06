**Recommendation**
Store lat/lon as a separate SPIRE coordinates artifact (TSV), then reference that TSV as an additional resource in the SPIRE datapackage descriptor.  
This is better than embedding coordinates into descriptor resource entries because it keeps the descriptor small, preserves one-to-many MAG-to-sample mappings, and is easier for downstream analytics in MinIO.

**Proposed Plan**
## Plan: SPIRE MAG Coordinate Enrichment

Build a reproducible coordinate-enrichment workflow that joins SPIRE study/sample API data with Metalog enrichment, writes a flat MAG-sample coordinates TSV, and registers that TSV in the SPIRE datapackage.

**Steps**
1. Phase 1: Discovery and schema lock
2. Confirm exact join keys and fields from SPIRE study/sample TSV endpoints: study, sample, MAG, latitude, longitude, plus available provenance fields.
3. Confirm Metalog mapping fields and define conflict policy for canonical coordinate selection.
4. Finalize output schema as one row per MAG-sample pair with provenance and version timestamps.
5. Phase 2: Coordinate extraction pipeline
6. Add a SPIRE coordinate builder script that loops studies → samples → MAGs, with retry/backoff, rate limiting, and checkpoint/resume.
7. Add Metalog enrichment join logic and precedence rules while retaining both source values where available.
8. Produce deterministic TSV output and a compact summary artifact with coverage/quality stats.
9. Phase 3: Descriptor and docs integration
10. Update descriptor generation to include the coordinates TSV as a dedicated resource.
11. Update SPIRE docs with generation workflow, expected runtime, and MinIO upload steps for the new artifact.
12. Document release/version caveats and provenance assumptions from SPIRE guidance.
13. Phase 4: Validation
14. Add tests for output schema, required columns, and deterministic row ordering.
15. Extend descriptor checks to ensure the coordinates resource is present and valid.
16. Run small-scope integration on a subset of studies before full execution.

**Relevant files**
- create_descriptor.py — add coordinates resource registration in descriptor assembly.
- README.md — add operator workflow for coordinates extraction and upload.
- notes.md — retain provenance assumptions and source caveats.
- descriptor_validator.py — reuse existing descriptor validation flow.
- minio_client.py — reuse upload patterns for MinIO object placement.
- test_descriptor_validator.py — extend validation coverage for new descriptor resource.
- test_nayfach_integration.py — reference auxiliary metadata artifact testing pattern.
- spire_v01_datapackage.json — expected output includes the new coordinates resource entry.

**Verification**
1. Unit tests for API parsing and MAG-sample join correctness.
2. Subset integration run to validate coverage and missing-coordinate handling.
3. Descriptor validation pass with the added coordinates TSV resource.
4. Spot QA against source SPIRE and Metalog records for provenance correctness.
5. MinIO object listing check confirming descriptor and coordinates artifact coexist under SPIRE prefix.

**Decisions captured**
- Source scope: SPIRE API plus Metalog enrichment.
- Multiplicity: one row per MAG-sample pair.
- Storage format: TSV artifact plus descriptor resource reference.
- Out of scope: embedding lat/lon directly inside per-genome descriptor entries.

This plan is persisted in session memory at /memories/session/plan.md and is ready for handoff to implementation.



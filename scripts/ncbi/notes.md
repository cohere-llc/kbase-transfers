Import updates - new release https://ncbiinsights.ncbi.nlm.nih.gov/2025/03/13/refseq-release-229/ 
3/31 - Matt: Detect new version
Assembly_summary.txt - has updates relative to last release, various tools can parse
Possible to could just re-run Matt’s script to ping md5 checksum
Check diffs
Auto-pull updates with new manifest (use CTS)
(later - wrap in docker)
Coordinate with AJ to trigger pull of new, additional CDM metadata
Automated Updates
Store assembly summary files with transferred data files for each version
Next Steps:
(AJ) to send Matt links on service development docs: ✅
API: https://berdl.kbase.us/apis/cts/docs#
Task Service repo (see README and docs directory): https://github.com/kbase/cdm-task-service 
(Matt) to create initial Dockerfile for automated transfers ✅
Draft container w/ sync scripts: https://github.com/cohere-llc/kbase-transfers/pull/1 
(Matt) to put together questions for Gavin about interaction with S3 store from containers ✅
Our use-case seems somewhat different than what is described in the CTS design doc. They seem to be targeting science users who need to do data processing of moderate+ complexity on staged input data, producing output data that is also staged prior to a separate operation that moves the output to its final destination. In our case, we are doing no data processing. We just need to transfer data from an external FTP server to the Lakehouse S3 store. Importantly we need access to existing object metadata (checksums) in the S3 store to determine which files are out-of-sync and therefore need to be transferred again (due to a version change, e.g.). The staging requirement would seem to prevent this in practical terms. Can we access the S3 store from our container, similar to what is possible in the JupyterLab environment?
Also, I did end up using the assembly summary file to figure out what folders exist. I misunderstood in the meeting that there is just one of these files for the whole RefSeq dataset. Traversing the FTP folders does take a decent amount of time in the current script, so it helps to have this file to avoid that step.

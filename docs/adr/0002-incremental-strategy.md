# ADR 0002 – Incremental strategy for fact_trips

**Status:** accepted

**Context.** Source files are monthly. A month can be replayed (same file) or re-delivered (same name, new
content). The source has no trip identifier; the only stable technical key is file name + row number.

**Options.**
A. `delete+insert` by period/file. B. `merge` on `trip_record_key`.

**Decision.** `delete+insert` with `unique_key = source_file`.
- Filter on incremental runs: files not yet present in `fact_trips`, plus files in `--vars reprocess_files`.
- The manifest flags a file `needs_reprocessing` when its SHA-256 changes; `run_pipeline.py transform` passes those
  names to dbt, which deletes that file's rows and re-inserts them.
- Not used as keys: pickup time, amount or distance (not unique).

**Why not merge.** A changed file can have a different number of rows, so rows would be left behind; the row key
embeds the row number, which shifts when content changes. Replacing the whole partition is simpler and provably
correct.

**Verification** (`tests/integration/test_pipeline.py`, `scripts/benchmark.py`):
replay changes no table; adding a month incrementally equals a full refresh over all months (row-level `EXCEPT`
over fact, dimensions, quarantine and all marts); a re-delivered file ends with no duplicate keys and equals a
clean full refresh.

**Known limit.** Marts are rebuilt as tables every run (they aggregate the whole fact table). Only the fact table
is incremental. Make marts incremental only if benchmarks on the full history show it is needed.

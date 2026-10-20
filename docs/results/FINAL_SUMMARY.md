# Final summary (2026-10-20)

Project: NYC TLC Yellow Taxi analytics engineering platform (Python + Parquet + DuckDB + dbt).

## Real-data run (56 files, 2022-01 to 2026-08, 197,562,001 rows, 3.29 GB)
Windows 11, 8 CPUs, 8.3 GB RAM, DuckDB limit 6 GB. Evidence: `docs/results/evidence/real-2022-2026_*`.

| Item | Result |
|---|---|
| Audit | 56/56 files readable, no gaps, no duplicate months, 3 schema-drift events |
| Build | 1,197 s, 434 dbt steps pass, 1 expected warning, 0 errors, DuckDB file 8.60 GB |
| Reconciliation | 8/8 checks PASS: source 197,562,001 = valid 197,539,104 + quarantined 22,897; fact = valid |
| Quarantine | 22,897 rows (0.0116%): 19,908 dropoff before pickup, 2,974 pickup outside file month, 15 exact duplicates |
| Warnings kept | 4,371,395 negative fares (2.21%; 5.85% in 2025, cause not investigated); 0 negative distances; 0 unmapped pickup zones (IDs 264/265 count as mapped) |
| Schema | `Airport_fee` case rename (2023-02), `cbd_congestion_fee` added (2025-01), `request_source` added (2026-06, not loaded) |

## Synthetic evidence (logic and scale, not data quality)
- 26+ pytest tests (unit and dbt integration), SQLFluff clean; integration run also passes on the author's Windows machine.
- Incremental output equals full refresh; replay is idempotent; by-file load equals single-pass.
- 225M synthetic rows load under a 2 GB limit in 1,258 s; 18M-row smoke test under 2 GB in 137 s.

## Not done / limits
- Benchmark of full refresh vs incremental on the real data was not run.
- By-file vs single-pass equivalence is proven on synthetic data only.
- One run on one machine; commit hash not recorded; duplicate detection is per file only.
- Negative-fare handling is a decision left to the project owner.

Details: `README.md`, `docs/results/real-2022-2026_2026-10-20.md`, `docs/adr/`.

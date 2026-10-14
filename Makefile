.PHONY: verify download audit raw-view transform report pipeline test sql-lint dbt-parse benchmark docs

download:
	python scripts/download_data.py --start 2022-01 --end 2026-12

audit:
	python scripts/run_pipeline.py audit

raw-view:
	python scripts/run_pipeline.py build-raw

transform:
	python scripts/run_pipeline.py transform

report:
	python scripts/run_pipeline.py report

pipeline:
	python scripts/run_pipeline.py all

test:
	pytest -q

sql-lint:
	sqlfluff lint warehouse/models warehouse/tests

dbt-parse:
	dbt parse --project-dir warehouse --profiles-dir warehouse

benchmark:
	python scripts/benchmark.py --new-files 1

docs:
	cd warehouse && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

verify:
	python scripts/verify_real_data.py --label real-2022-2026

"""The memory bound only works if pipeline_lib's env var names match warehouse/profiles.yml."""
import re
from pathlib import Path

import pipeline_lib as lib

PROFILES = Path(lib.WAREHOUSE / "profiles.yml").read_text(encoding="utf-8")


def test_every_profile_env_var_is_set_by_the_pipeline(tmp_path):
    wanted = set(re.findall(r"env_var\('([A-Z_]+)'", PROFILES))
    assert {"DBT_DUCKDB_MEMORY_LIMIT", "DBT_THREADS", "DBT_DUCKDB_PATH", "DBT_DUCKDB_TEMP"} <= wanted
    env = lib.dbt_env(tmp_path / "wh.duckdb")
    missing = wanted - set(env)
    assert not missing, f"profiles.yml reads {missing} but the pipeline never sets them"


def test_user_override_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "3GB")
    assert lib.dbt_env(tmp_path / "wh.duckdb")["DBT_DUCKDB_MEMORY_LIMIT"] == "3GB"

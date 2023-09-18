import pytest
from dbt.tests.util import run_dbt_and_capture, run_dbt

from tests.functional.show.test_show import ShowBase
from tests.functional.show.fixtures import models__second_ephemeral_model


# -- Below we define base classes for tests you import based on if your adapter supports dbt show or not --
class BaseShowLimit(ShowBase):
    @pytest.mark.parametrize(
        "args,expected",
        [
            ([], 5),  # default limit
            (["--limit", 3], 3),  # fetch 3 rows
            (["--limit", -1], 7),  # fetch all rows
        ],
    )
    def test_limit(self, project, args, expected):
        run_dbt(["build"])
        dbt_args = ["show", "--inline", models__second_ephemeral_model, *args]
        results = run_dbt(dbt_args)
        assert len(results.results[0].agate_table) == expected
        # ensure limit was injected in compiled_code when limit specified in command args
        limit = results.args.get("limit")
        if limit > 0:
            assert f"limit {limit}" in results.results[0].node.compiled_code


class BaseShowSqlHeader(ShowBase):
    def test_sql_header(self, project):
        run_dbt(["build", "--vars", "timezone: Asia/Kolkata"])
        (_, log_output) = run_dbt_and_capture(
            ["show", "--select", "sql_header", "--vars", "timezone: Asia/Kolkata"]
        )


class TestPostgresShowSqlHeader(BaseShowSqlHeader):
    pass


class TestPostgresShowLimit(BaseShowLimit):
    pass

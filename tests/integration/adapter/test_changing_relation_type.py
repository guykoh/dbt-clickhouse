import os
from typing import List, Optional

import pytest
from dbt.tests.adapter.relations.test_changing_relation_type import BaseChangeRelationTypeValidator
from dbt.tests.util import run_dbt


class TestChangeRelationTypes(BaseChangeRelationTypeValidator):
    pass


class TestChangeRelationTypesWithDistributedMaterializations(BaseChangeRelationTypeValidator):

    # changing relation from distributed to non-distrubted should raise compilation error
    # unless with a full-refresh flag
    def _run_and_check_materialization_error(
        self, materialization, extra_args: Optional[List] = None
    ):
        run_args = ["run", '--vars', f'materialized: {materialization}']
        if extra_args:
            run_args.extend(extra_args)
        results = run_dbt(run_args, expect_pass=False)
        assert results[0].status == "error"
        assert "Incompatible relation status" in results[0].message

    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_CLUSTER', '').strip() == '', reason='Not on a cluster'
    )
    def test_changing_materialization_changes_relation_type(self, project):
        self._run_and_check_materialization('view')
        self._run_and_check_materialization('distributed_table')
        self._run_and_check_materialization('distributed_incremental')
        self._run_and_check_materialization_error('table')
        self._run_and_check_materialization('table', extra_args=['--full-refresh'])
        self._run_and_check_materialization(
            'distributed_incremental', extra_args=['--full-refresh']
        )
        self._run_and_check_materialization_error('incremental')
        self._run_and_check_materialization('incremental', extra_args=['--full-refresh'])
        self._run_and_check_materialization('distributed_table', extra_args=['--full-refresh'])

from pathlib import Path
from collections.abc import Iterable
import inspect
from functools import cache

import sqlalchemy as sa

from co3 import util
from co3.accessor import Accessor
from co3.relation import Relation

#from co3.databases.sql import RelationalDatabase, TabularDatabase, SQLDatabase
from co3.relations import TabularRelation, SQLTable


class RelationalAccessor[D: 'RelationalDatabase', R: Relation](Accessor[D]):
    pass


class TabularAccessor[D: 'TabularDatabase', R: TabularRelation](RelationalAccessor[D, R]):
    pass


class SQLAccessor(TabularAccessor['SQLDatabase', SQLTable]):
    def raw_select(
        self,
        sql,
        bind_params=None,
        mappings=False,
        include_cols=False,
    ):
        res_method = utils.db.sa_exec_dicts
        if mappings:
            res_method = utils.db.sa_exec_mappings

        return res_method(self.database.engine, sa.text(sql), bind_params=bind_params, include_cols=include_cols)

    def select(
        self,
        table: sa.Table | sa.Subquery | sa.Join,
        cols         = None,
        where        = None,
        distinct_on  = None,
        order_by     = None,
        limit        = 0,
        mappings     = False,
        include_cols = False,
    ):
        '''
        Perform a SELECT query against the provided table-like object (see
        `check_table()`).

        Deprecated: String aliases
            String aliases for tables are no longer supported. This method no longer checks
            against any specific schema table-maps or Composers. Instead, this should be
            done outside the Accessor.

        Parameters:
            group_by: list of columns to group by; for now serves as a proxy for DISTINCT
                      (no aggregation methods accepted)
            order_by: column to order results by (can use <col>.desc() to order
                      by descending)
        '''
        if where is None:
            where = sa.true()

        res_method = utils.db.sa_exec_dicts
        if mappings:
            res_method = utils.db.sa_exec_mappings

        stmt = sa.select(table).where(where)
        if cols is not None:
            stmt = sa.select(*cols).select_from(table).where(where)

        if distinct_on is not None:
            stmt = stmt.group_by(distinct_on)

        if order_by is not None:
            stmt = stmt.order_by(order_by)

        if limit > 0:
            stmt = stmt.limit(limit)

        return res_method(self.engine, stmt, include_cols=include_cols)

    def select_one(self, table, cols=None, where=None, mappings=False, include_cols=False):
        res = self.select(table, cols, where, mappings, include_cols, limit=1)

        if include_cols and len(res[0]) > 0:
            return res[0][0], res[1]

        if len(res) > 0:
            return res[0]

        return None


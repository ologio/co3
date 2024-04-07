'''
Design proposal: variable backends

One particular feature not supported by the current type hierarchy is the possible use of
different backends to implement a general interface like SQLAccessor. One could imagine,
for instance, using `sqlalchemy` or `sqlite` to define the same methods laid out in a
parent class blueprint. It's not too difficult to imagine cases where both of these may be
useful, but for now it is outside the development scope. Should it ever enter the scope,
however, we might consider a simple `backend` argument on instantiation, keeping just the
SQLAccessor exposed rather than a whole set of backend-specific types:

```py
class SQLAlchemyAccessor(RelationalAccessor): # may also inherit from a dedicated interface parent
    def select(...):
        ...

class SQLiteAccessor(RelationalAccessor): 
    def select(...):
        ...

class SQLAccessor(RelationalAccessor): 
    backends = {
        'sqlalchemy': SQLAlchemyAccessor,
        'sqlite':     SQLteAccessor,
    }

    def __init__(self, backend: str):
        self.backend = self.backends.get(backend)

    def select(...):
        return self.backend.select(...)

```

For now, we can look at SQLAccessor (and equivalents in other type hierarchies, like
SQLManagers) as being SQLAlchemyAccessors and not supporting any backend swapping. But in
theory, to make the above change, we'd just need to rename it and wrap it up.
'''

from pathlib import Path
from collections.abc import Iterable
import inspect
from functools import cache

import sqlalchemy as sa

from co3 import util
from co3.accessor import Accessor
from co3.components import Relation, SQLTable


class RelationalAccessor[R: Relation, D: 'RelationalDatabase[R]'](Accessor[R, D]):
    def raw_select(self, sql: str):
        raise NotImplementedError

    def select(
        self,
        relation: R,
        cols         = None,
        where        = None,
        distinct_on  = None,
        order_by     = None,
        limit        = 0,
    ):
        raise NotImplementedError

    def select_one(
        self,
        relation     : R,
        cols                = None,
        where               = None,
        mappings     : bool = False,
        include_cols : bool = False,
    ):
        res = self.select(relation, cols, where, mappings, include_cols, limit=1)

        if include_cols and len(res[0]) > 0:
            return res[0][0], res[1]

        if len(res) > 0:
            return res[0]

        return None


class SQLAccessor(RelationalAccessor[SQLTable, 'SQLDatabase[SQLTable]']):
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
        table: SQLTable,
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

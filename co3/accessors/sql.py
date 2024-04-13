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
from co3.engines import SQLEngine
from co3.accessor import Accessor
from co3.components import Relation, SQLTable


class RelationalAccessor[R: Relation](Accessor[R]):
    def raw_select(
        self, 
        connection,
        text: str
    ):
        connection.exec
        raise NotImplementedError

    def select(
        self,
        connection,
        relation: R,
        attributes   = None,
        where        = None,
        distinct_on  = None,
        order_by     = None,
        limit        = 0,
        mappings     : bool = False,
        include_cols : bool = False,
    ):
        raise NotImplementedError

    def select_one(
        self,
        connection,
        relation     : R,
        attributes          = None,
        where               = None,
        mappings     : bool = False,
        include_cols : bool = False,
    ):
        res = self.select(
            relation, attributes, where, mappings, include_cols, limit=1)

        if include_cols and len(res[0]) > 0:
            return res[0][0], res[1]

        if len(res) > 0:
            return res[0]

        return None


class SQLAccessor(RelationalAccessor[SQLTable]):
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
        table:       SQLTable,
        columns      = None,
        where        = None,
        distinct_on  = None,
        order_by     = None,
        limit        = 0,
        mappings     = False,
        include_cols = False,
    ): # -> list[dict|sa.Mapping]: (double check the Mapping types)
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

        Returns:
            Statement results, either as a list of 1) SQLAlchemy Mappings, or 2) converted
            dictionaries
        '''
        if where is None:
            where = sa.true()

        stmt = sa.select(table).where(where)
        if cols is not None:
            stmt = sa.select(*cols).select_from(table).where(where)

        if distinct_on is not None:
            stmt = stmt.group_by(distinct_on)

        if order_by is not None:
            stmt = stmt.order_by(order_by)

        if limit > 0:
            stmt = stmt.limit(limit)

        res = SQLEngine._execute(connection, statement, include_cols=include_cols)

        if mappings:
            return res.mappings().all()
        else:
            return self.result_dicts(res)

    @staticmethod
    def result_dicts(results, query_cols=None):
        '''
        Parse SQLAlchemy results into Python dicts. Leverages mappings to associate full
        column name context.

        If `query_cols` is provided, their implicit names will be used for the keys of the
        returned dictionaries. This information is not available under CursorResults and thus
        must be provided separately. This will yield results like the following:

        [..., {'table1.col':<value>, 'table2.col':<value>, ...}, ...]

        Instead of the automatic mapping names:

        [..., {'col':<value>, 'col_1':<value>, ...}, ...]

        which can make accessing certain results a little more intuitive. 
        '''
        result_mappings = results.mappings().all()

        if query_cols:
            return [
                { str(c):r[c] for c in query_cols }
                for r in result_mappings
            ]

        return [dict(r) for r in result_mappings]

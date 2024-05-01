import sqlalchemy as sa

from co3 import util
from co3.accessor import Accessor
from co3.accessors.sql import SQLAccessor


class FTSAccessor(Accessor):
    '''
    Perform queries on efficient full-text search (FTS) tables.

    Note how this class doesn't inherit from ``SQLAccessor``, or even
    ``RelationalAccessor``. We don't look at FTS tables as Relations, due to restrictions
    on their JOIN capabilities, and avoid embracing what composability they may have to
    prevent accidentally introducing inefficiencies. Instead, just single FTS tables can
    be selected from via this Accessor, and the FTSTable type, despite the use of the word
    "table", is a direct child of the Component type.
    '''
    def __init__(self):
        self.sql_accessor = SQLAccessor()
        self.access_log   = sql_accessor.access_log

    def select(
        self,
        connection,
        table_name  : str,
        select_cols : str | list | None = '*',
        search_cols : str | None        = None,
        query       : str | None        = None,
        col_query   : str | None        = None,
        snip_col    : int | None        = 0,
        hl_col      : int | None        = 0,
        limit       : int | None        = 100,
        snip        : int | None        = 64,
        tokenizer   : str | None        = 'unicode61',
    ):
        '''
        Execute a search query against an indexed FTS table for specific primitives. This
        method is mostly a generic FTS handler, capable of handling queries to any available
        FTS table with a matching naming scheme (``fts_<type>_<tokenizer>``). The current
        intention is support all tokenizers, for file, note, block, and link primitives.

        Search results include all FTS table columns, as well as SQLite-supported ``snippet``s
        and ``highlight``s for matches. Matches are filtered and ordered by SQLite's
        ``MATCH``-based score for the text & column queries. Results are (a list of) fully
        expanded dictionaries housing column-value pairs.

        Parameters:
            table_name  : name of FTS table to search
            search_cols : space separated string of columns to use for primary queries
            q           : search query
            colq        : column constraint string; must conform to SQLite standards (e.g.,
                          ``<col>:<text>``
            snip_col    : table column to use for snippets (default: 1; source content column)
            hl_col      : table column to use for highlights (default: 2; format column, applied
                          to HTML targets)
            limit       : maximum number of results to return in the SQL query
            snip        : snippet length (max: 64)
            tokenizer   : tokenizer to use (assumes relevant FTS table has been built)

        Returns:
            Dictionary with search results (list of column indexed dictionaries) and relevant
            metadata.
        '''
        search_query = ''

        if type(select_cols) is list:
            select_cols = ', '.join(select_cols)

        # construct main search query
        if search_cols and query:
            search_query = f'{{{search_cols}}} : {query}'

        # add auxiliary search constraints
        if col_query:
            search_query += f' {col_query}'

        search_query = search_query.strip()

        hl_start = '<b><mark>'
        hl_end   = '</mark></b>'

        fts_table_name = f'{table_name}_fts_{tokenizer}'
        
        sql = f'''
        SELECT
            {select_cols},
            snippet({fts_table_name}, {snip_col}, '{hl_start}', '{hl_end}', '...', {snip}) AS snippet,
            highlight({fts_table_name}, {hl_col}, '{hl_start}', '{hl_end}') AS highlight 
        FROM {fts_table_name}
        '''
        
        where_clauses = []
        if search_query:
            where_clauses.append(f"{fts_table_name} MATCH '{search_query}'\n")

        if wherein_dict:
            for col, vals in wherein_dict.items():
                where_clauses.append(f'{col} IN {tuple(vals)}\n')

        if where_clauses:
            where_str = " AND ".join(where_clauses)
            sql += f'WHERE {where_str}'

        sql += f'ORDER BY rank LIMIT {limit};'

        row_dicts, cols = self.sql_accessor.raw_select(connection, sql, include_cols=True)

        return row_dicts, cols

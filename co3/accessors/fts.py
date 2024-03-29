import sqlalchemy as sa

from co3 import util
from co3.accessor import Accessor


class FTSAccessor(Accessor):
    def search(
        self,
        table_name  : str,
        select_cols : str  | list | None = '*',
        search_cols : str  | None = None,
        q           : str  | None = None,
        colq        : str  | None = None,
        snip_col    : int  | None = 0,
        hl_col      : int  | None = 0,
        limit       : int  | None = 100,
        snip        : int  | None = 64,
        tokenizer   : str  | None = 'unicode61',
        group_by    : str  | None = None,
        agg_cols    : list | None = None,
        wherein_dict: dict | None = None,
        unique_on   : dict | None = None,
    ):
        '''
        Execute a search query against an indexed FTS table for specific primitives. This
        method is mostly a generic FTS handler, capable of handling queries to any available
        FTS table with a matching naming scheme (`fts_<type>_<tokenizer>`). The current
        intention is support all tokenizers, for file, note, block, and link primitives.

        Search results include all FTS table columns, as well as SQLite-supported `snippet`s
        and `highlight`s for matches. Matches are filtered and ordered by SQLite's
        `MATCH`-based score for the text & column queries. Results are (a list of) fully
        expanded dictionaries housing column-value pairs.

        Note:
            GROUP BY cannot be paired with SQLITE FTS extensions; thus, we perform manual
            group checks on the result set in Python before response

        Analysis:
            The returned JSON structure has been (loosely) optimized for speed on the client
            side. Fully forming individual dictionary based responses saves time in
            Javascript, as the JSON parser is expected to be able to create the objects
            faster than post-hoc construction in JS. This return structure was compared
            against returning an array of arrays (all ordered in the same fashion), along with
            a column list to be associated with each of the result values. While this saves
            some size on the payload (the same column names don't have to be transmitted for
            each result), the size of the returned content massively outweighs the
            predominantly short column names. The only way this structure would be viable is
            if a significant amount was saved on transfer compared to the slow down in JS
            object construction; this is (almost) never the case.

        Parameters:
            table_name  : name of FTS table to search
            search_cols : space separated string of columns to use for primary queries
            q           : search query
            colq        : column constraint string; must conform to SQLite standards (e.g.,
                          `<col>:<text>`
            snip_col    : table column to use for snippets (default: 1; source content column)
            hl_col      : table column to use for highlights (default: 2; format column, applied
                          to HTML targets)
            limit       : maximum number of results to return in the SQL query
            snip        : snippet length (max: 64)
            tokenizer   : tokenizer to use (assumes relevant FTS table has been built)
            ...
            wherein_dict: (col-name, value-list) pairs to match result set against, via
                          WHERE ... IN clauses

        Returns:
            Dictionary with search results (list of column indexed dictionaries) and relevant
            metadata.
        '''
        search_q = ''

        if type(select_cols) is list:
            select_cols = ', '.join(select_cols)

        # construct main search query
        if search_cols and q:
            search_q = f'{{{search_cols}}} : {q}'

        # add auxiliary search constraints
        if colq:
            search_q += f' {colq}'

        search_q = search_q.strip()

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
        if search_q:
            where_clauses.append(f"{fts_table_name} MATCH '{search_q}'\n")

        if wherein_dict:
            for col, vals in wherein_dict.items():
                where_clauses.append(f'{col} IN {tuple(vals)}\n')

        if where_clauses:
            where_str = " AND ".join(where_clauses)
            sql += f'WHERE {where_str}'

        sql += f'ORDER BY rank LIMIT {limit};'

        row_dicts, cols = self.raw_select(sql, include_cols=True)

        if group_by is None:
            return row_dicts, cols

        if agg_cols is None:
            agg_cols = []

        # "group by" block ID and wrangle the links into a list
        # note we can't perform native GROUP BYs with FTS results
        group_by_idx = {}
        for row in row_dicts:
            group_by_attr = row.get(group_by)

            # add new entries
            for agg_col in agg_cols:
                row[f'{agg_col}_agg'] = set()

            if group_by_attr is None:
                continue

            if group_by_attr not in group_by_idx:
                group_by_idx[group_by_attr] = row

            for agg_col in agg_cols:
                if agg_col in row:
                    group_by_idx[group_by_attr][f'{agg_col}_agg'].add(row[agg_col])

        return {
            'results'     : group_by_idx,
            'columns'     : cols,
            'num_results' : len(row_dicts),
        }

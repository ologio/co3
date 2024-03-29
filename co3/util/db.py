'''
Example usage for this file's utilities:

# get SA engine, creating folder hierarchy to provided DB path
engine = db.get_engine(<path>)

# execute a single SA statement, returns a CursorResult
select_results = db.sa_execute(engine, sa.select(<table>))

# convert raw results to dictionaries, keys corresponding to col names
select_dicts = db.named_results(<table>, select_results)

# use table defaults and cols to create compliant insert
insert_dicts = [ db.prepare_insert(<table>, sd) for sd in select_dicts ]

# perform a bulk insert
with engine.connect() as connection:
    connection.execute(
        sa.insert(<table>),
        insert_dicts
    )
'''

import time
import logging
import functools
import sqlalchemy as sa
from pathlib import Path


logger = logging.getLogger(__name__)

def get_engine(db_path, echo=False):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return sa.create_engine(f"sqlite:///{db_path}", echo=echo)

def named_results(table, results):
    '''
    Note the implications of this for results from compound tables containing the same
    column names: only the last column name will be indexed.
    '''
    return [
        { c.name:r[i] for i, c in enumerate(table.columns) }
        for r in results
    ]

# RAW CURSOR-RESULT MANIPULATION -- SEE SA-PREFIXED METHODS FOR CONN-WRAPPED COUNTERPARTS
def result_mappings(results):
    return results.mappings()

def result_mappings_all(results):
    return result_mappings(results).all()

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
    if query_cols:
        return [
            { str(c):r[c] for c in query_cols }
            for r in result_mappings_all(results)
        ]

    return [dict(r) for r in result_mappings_all(results)]

def sa_execute(engine, stmt, bind_params=None, include_cols=False):
    '''
    Simple single-statement execution in a with-block
    '''
    with engine.connect() as conn:
        res = conn.execute(stmt, bind_params)

        if include_cols:
            cols = list(res.mappings().keys())
            return res, cols

        return res

def sa_exec_mappings(engine, stmt, bind_params=None, include_cols=False):
    '''
    All mappings fetched inside of connect context, safe to access outside
    '''
    with engine.connect() as conn:
        res = conn.execute(stmt, bind_params)
        mappings = result_mappings_all(res)

        if include_cols:
            cols = list(res.mappings().keys())
            return mappings, cols

        return mappings

def sa_exec_dicts(engine, stmt, bind_params=None, include_cols=False):
    with engine.connect() as conn:
        res = conn.execute(stmt, bind_params)
        dicts = result_dicts(res)

        if include_cols:
            cols = list(res.mappings().keys())
            return dicts, cols

        return dicts

def sa_exec_explicit(engine, stmt, bind_params=None):
    with engine.connect() as conn:
        trans = conn.begin()  # start a new transaction explicitly
        try:
            result = conn.execute(stmt, bind_params)
            trans.commit()  # commit the transaction explicitly
            return result
        except:
            trans.rollback()  # rollback the transaction explicitly
            raise

def prepare_insert(table, value_dict):
    '''
    Modifies insert dictionary with full table column defaults
    '''
    insert_dict = get_column_defaults(table)
    #insert_dict.update(value_dict)
    insert_dict.update(
        { k:v for k,v in value_dict.items() if k in insert_dict }
    )

    return insert_dict

def deferred_fkey(target, **kwargs):
    return sa.ForeignKey(
        target,
        deferrable=True,
        initially='DEFERRED',
        **kwargs
    )

def deferred_cd_fkey(target, **kwargs):
    '''
    Prefer this when using FKEYs; need to really justify _not_ having a CASCADE deletion
    enabled
    '''
    return deferred_fkey(target, ondelete='CASCADE', **kwargs)

def get_column_defaults(table, include_all=True):
    '''
    Provide column:default pairs for a provided SQLAlchemy table.

    Parameters:
        table: SQLAlchemy table
        include_all: whether to include all columns, even those without explicit defaults
    '''
    default_values = {}
    for column in table.columns:
        if column.default is not None:
            default_values[column.name] = column.default.arg
        elif column.nullable:
            default_values[column.name] = None
        else:
            # assume empty string if include_all and col has no explicit default 
            # and isn't nullable
            if include_all and column.name != 'id':
                default_values[column.name] = ''

    return default_values

def get_column_names_str_table(engine, table: str):
    col_sql = f'PRAGMA table_info({table});'
    with engine.connect() as connection:
        try:
            cols = connection.execute(sa.text(col_sql))
        except sa.exc.OperationalError as e:
            logger.error(f'Column retrieval for table "{table}" failed')
            raise

    return cols

def create_fts5(
        engine,
        table: sa.Table | str,
        columns=None,
        populate=False,
        inserts=None,
        reset_fts=False,
        tokenizer='unicode61',
    ):
    '''
    Create and optionally populate an FTS5 table in SQLite. Can be used directly for
    existing tables in the same database. It can also be used for composite tables (i.e.,
    those created from JOINs) or really any other data by providing explicit inserts and
    column names to use during population.

    Parameters:
        table: either SQLAlchemy table instance, or table name string
        columns: list of SQLAlchemy table columns to insert into virtual table. These
                 columns must be present in the provided table if not manually specifying
                 inserts (since the table must be queried automatically)
        inserts: 
    '''
    is_sa_table = isinstance(table, sa.Table)
    table_name  = table.name if is_sa_table else table

    if columns is None:
        if is_sa_table:
            columns = [c.name for c in table.c] 
        else:
            columns = get_column_names_str_table(engine, table)

    col_str = ", ".join(columns)
    fts_table_name = f'{table_name}_fts_{tokenizer}'

    sql = f"""
    CREATE VIRTUAL TABLE {fts_table_name} USING fts5
    (
        {col_str},
        tokenize = '{tokenizer}'
    );
    """

    sql_insert = f"""
    INSERT INTO {fts_table_name}
    (
        {col_str}
    )
    """
    if inserts is None:
        sql_insert += f"""
            SELECT {col_str}
            FROM {table_name};
        """
    else:
        sql_insert += f"""
        VALUES ({', '.join(':' + c for c in columns)})
        """

    sql_drop = f"DROP TABLE IF EXISTS {fts_table_name}"

    with engine.connect() as connection:
        if reset_fts:
            connection.execute(sa.text(sql_drop))

        connection.execute(sa.text(sql))

        if populate:
            if inserts is None:
                connection.execute(sa.text(sql_insert))
            else:
                connection.execute(
                    sa.text(sql_insert),
                    inserts,
                )

        connection.commit()

def populate_fts5(engine, tables, columns=None, inserts=None):
    # create indexes
    tokenizers = ['unicode61', 'porter', 'trigram']

    for table in tables:
        for tokenizer in tokenizers:
            start = time.time()
            create_fts5(
                engine,
                table,
                columns=columns,
                populate=True,
                inserts=inserts,
                reset_fts=True,
                tokenizer=tokenizer
            )

            is_sa_table = isinstance(table, sa.Table)
            table_name  = table.name if is_sa_table else table
            print(f'Created FTS5 index for table "{table_name}+{tokenizer}"; took {time.time() - start}s')


def create_vss0(
        engine,
        table: sa.Table | str,
        columns=None,
        populate=False,
        inserts=None,
        reset=False,
        embedding_size=384,
    ):
    '''
    Create a VSS table

    Parameters:
        table: either SQLAlchemy table instance, or table name string
        columns: list of SQLAlchemy table columns to insert into virtual table. These
                 columns must be present in the provided table if not manually specifying
                 inserts (since the table must be queried automatically)
        inserts: 
    '''
    is_sa_table = isinstance(table, sa.Table)
    table_name  = table.name if is_sa_table else table

    if columns is None:
        if is_sa_table:
            columns = [c.name for c in table.c] 
        else:
            columns = get_column_names_str_table(engine, table)

    col_str = ", ".join(columns)
    vss_table_name = f'{table_name}_vss'

    sql = f"""
    CREATE VIRTUAL TABLE {vss_table_name} USING vss0
    (
        chunk_embedding({embedding_size}),
        query_embedding({embedding_size}),
    );
    """

    sql_insert = f"""
    INSERT INTO {vss_table_name}
    (
        rowid, chunk_embedding
    )
    """
    if inserts is None:
        sql_insert += f"""
            SELECT {col_str}
            FROM {table_name};
        """
    else:
        sql_insert += f"""
        VALUES ({', '.join(':' + c for c in columns)})
        """

    sql_drop = f"DROP TABLE IF EXISTS {vss_table_name}"

    with engine.connect() as connection:
        if reset:
            connection.execute(sa.text(sql_drop))

        connection.execute(sa.text(sql))

        if populate:
            if inserts is None:
                connection.execute(sa.text(sql_insert))
            else:
                connection.execute(
                    sa.text(sql_insert),
                    inserts,
                )

        connection.commit()

def fts5_prep_composite(engine, table, table_name, columns=None):
    '''
    Helper method for prepping JOIN tables for FTS5 creation.
    '''
    table.name = table_name

    rows, cols = utils.db.sa_execute(
        engine,
        sa.select(*select_cols).select_from(all_search),
        include_cols=True
    )
    rows = utils.db.result_dicts(rows)

    return table, rows, cols


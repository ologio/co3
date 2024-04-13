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


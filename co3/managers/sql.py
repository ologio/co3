'''
Note: Common on insert behavior
    - Tables with unique constraints have been equipped with `sqlite_on_conflict_unique`
      flags, enabling conflicting bulk inserts to replace conflicting rows gracefully. No
      need to worry about explicitly handling upserts.
    - The bulk insert via conn.execute(<insert>,<row_list>) automatically ignores
      irrelevant column names within provided record dicts, whereas explicit .values() calls
      for non-bulk inserts will throw errors if not aligned perfectly. We want to keep these
      bulk calls AND allow update/replacements on conflict; the setting above allows this
      bulk usage to continue as is.

Note: Options for insert/update model
    1. Collect all possible update objects, and use a SQLite bulk insert that only updates
       if needed (based on modified time, for example). This sounds on the surface like
       the right approach since we defer as much to bulk SQL logic, but it's far and away
       the worse option b/c prepping all file/note/etc objects is too expensive to
       ultimately throw away when finding out an update isn't needed. For example, if we
       wanted to perform note updates _inside_ the SQL call (like an `INSERT .. UPDATE ..
       IF`, as opposed to determining the objects to insert _outside_ of the SQL call),
       you would need to bring each of the note's HTML prior to the insert check. There's
       no 2-stage processing here where you can check if the note needs to be converted
       b/c it's out of date, and only then perform the computation.
    2. Instantiate objects sequentially, each time checking with the DB to see if full
       processing is needed. This makes much more sense when the full processing is very
       expensive, as it is with Note conversion. This would iterate through available notes,
       perform a `SELECT` on the target table to see if the note needs updating, and if so
       perform the remaining computation. Those objects then get added to a "update object
       list" to be inserted in bulk, but you make sequential `SELECT` checks before that.
    
       The one extra optimization you could maybe make here is doing a full SELECT on the
       target table and bring all rows into memory before iterating through the objects.
       This would likely make it faster than whatever SQLAlchemy overhead there may be. It
       also might just be outright required given Connection objects aren't thread-safe;
       we can get away with single thread global SELECT, threaded checking during object
       build, then single thread bulk INSERT. (**Note**: this is what the method does).
'''

from pathlib import Path
from collections import defaultdict
import logging
import threading
import math
import time
import pprint
from concurrent.futures import wait, as_completed

import sqlalchemy as sa
from tqdm.auto import tqdm

from co3 import util
from co3.schema import Schema
from co3.manager import Manager
from co3.components import Relation, SQLTable

#from localsys.reloader.router._base import ChainRouter, Event


logger = logging.getLogger(__name__)


class RelationalManager[R: Relation](Manager[R]):
    pass


class SQLManager(RelationalManager[SQLTable]):
    '''
    Core schema table manager. Exposes common operations and facilitates joint operations
    needed for highly connected schemas.

    In particular, Managers expose insertion abstractions that take table-indexed groups
    of rows and bundle them under a single transaction. This is important for table groups
    with foreign keys and cascading deletions: inserts need to be coordinated. Note that
    actually collecting the inserts that need to take place is outside the scope of the
    Manager (see the Collector). We do, however, implement a `sync` operation that can
    saturates a router with events (dynamically) and sweeps up inserts on session basis
    from an attached collector.
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.routers  = []

        self._router    = None
        self._insert_lock = threading.Lock()

    @property
    def router(self):
        if self._router is None:
            self._router = ChainRouter(self.routers)
        return self._router

    def add_router(self, router):
        self.routers.append(router)

    def recreate(self, schema: Schema[SQLTable]): 
        schema.metadata.drop_all(self.engine)
        schema.metadata.create_all(self.engine, checkfirst=True)

    def update(self): pass

    def insert(self, inserts: dict):
        '''
        Perform provided table inserts.

        Parameters:
            inserts: table-indexed dictionary of insert lists
        '''
        total_inserts = sum([len(ilist) for ilist in inserts.values()])
        if total_inserts < 1: return

        logger.info(f'Total of {total_inserts} sync inserts to perform')

        # TODO: add some exception handling? may be fine w default propagation
        start = time.time()
        with self.engine.connect() as connection:
            with self._insert_lock:
                for table_str in inserts:
                    table_inserts = inserts[table_str]
                    if len(table_inserts) == 0: continue

                    table = tables.table_map[table_str]

                    logger.info(
                        f'Inserting {len(table_inserts)} out-of-date entries into table "{table_str}"'
                    )

                    connection.execute(
                        sa.insert(table),
                        table_inserts
                    )
                connection.commit()
                logger.info(f'Insert transaction completed successfully in {time.time()-start:.2f}s')

    def _file_sync_bools(self):
        synced_bools = []
        fpaths = utils.paths.iter_nested_paths(self.collector.basepath, no_dir=True)
        db_rpath_index = self.database.files.group_by('rpath')()
        
        for fpath in fpaths:
            file = File(fpath, self.collector.basepath)
            db_file = db_rpath_index.get(file.rpath)

            file_in_sync = db_file and float(db_file.get('mtime','0')) >= file.mtime
            synced_bools.append(file_in_sync)

        return fpaths, synced_bools

    def sync(
        self,
        limit=0,
        chunk_time=0,
        dry=False,
        chunk_size_cap=1000,
        print_report=True,
    ):
        '''
        Note: 
            Could be dangerous if going from fast file processing to note processing.
            Imagine estimating 1000 iter/sec, then transferring that to the next batch
            when it's more like 0.2 iter/sec. We would lose any chunking. (Luckily, in
            practice, turns out that notes are almost always processed before the huge set
            of nested files lurking and we don't experience this issue.)
        '''
        #filepaths, update_bools = self.collector.file_inserts(dry_run=True)
        #fpaths_to_update = [f for (f, b) in zip(filepaths, update_bools) if not b] 
        filepaths, update_bools = self._file_sync_bools()
        fpaths_to_update = [f for (f, b) in zip(filepaths, update_bools) if not b] 

        def calc_chunk_size(sec_per_file):
            '''
            Chunk cap only applied here; if manually set elsewhere in the method, it's
            intentional e.g., if chunk_time <= 0 and we just want one big iteration.
            '''
            return min(max(int(chunk_time / sec_per_file), 1), chunk_size_cap)
        
        # nothing to do
        if not fpaths_to_update:
            logger.info('Sync has nothing to do, exiting')
            return None

        ood_count = len(fpaths_to_update)
        total_fps = len(filepaths) 
        ood_prcnt = ood_count/total_fps*100
        logger.info(
            f'Pre-sync scan yielded {ood_count}/{total_fps} ({ood_prcnt:.2f}%) out-of-date files'
        )

        if limit <= 0: limit = ood_count

        if chunk_time <= 0:
            chunk_size = ood_count
        else:
            chunk_size = calc_chunk_size(5)

        dry_inserts = None
        remaining = limit

        chunk_time_used = 0
        pbar = tqdm(
            desc=f'Adaptive chunked sync for [limit {limit}]',
            total=remaining,
        )

        report = []
        def collect_report():
            report.append({
                'chunk_size': chunk_limit,
                'chunk_time_used': chunk_time_used,
            })

        with pbar as _:
            while remaining > 0:
                time_pcnt = chunk_time_used / max(chunk_time,1) * 100
                pbar.set_description(
                    f'Adaptive chunked sync [size {chunk_size} (max {chunk_size_cap})] '
                    f'[prev chunk {chunk_time_used:.2f}s/{chunk_time}s ({time_pcnt:.2f}%)]'
                )

                chunk_limit = min(remaining, chunk_size)
                start_idx = limit - remaining
                chunk_fpaths = fpaths_to_update[start_idx:start_idx+chunk_limit]

                chunk_time_start = time.time()
                #inserts = self.collector.collect_inserts(
                #    chunk_fpaths,
                #    skip_updated=False, # saves on DB check given provided list
                #)

                # 1) flush synthetic events for the batch through the chained router
                # 2) block until completed and sweep up the collected inserts
                futures = self.router.submit([
                    Event(
                        endpoint=str(self.collector.basepath),
                        name=str(path.relative_to(self.collector.basepath)),
                        action=[0xffffffff], # synthetic action to match any flag filters
                    ) for path in chunk_fpaths
                ])

                for future in tqdm(
                    as_completed(futures),
                    total=chunk_size,
                    desc=f'Awaiting chunk futures [submitted {len(futures)}/{chunk_size}]'
                ):
                    #print('Finished future', cfuture)
                    try:
                        result = future.result()
                    except Exception as e:
                        logger.warning(f"Sync job failed with exception {e}")

                #wait(futures)#, timeout=10)
                inserts = self.collector.collect_inserts()

                chunk_time_used = time.time() - chunk_time_start

                collect_report()
                #if not inserts: break

                if dry:
                    if dry_inserts is None:
                        dry_inserts = inserts
                    else:
                        for ik, iv in inserts.items():
                            dry_inserts[ik].extend(iv)
                else:
                    self.insert(inserts)

                remaining -= chunk_limit

                # re-calculate chunk size
                sec_per_file = chunk_time_used / chunk_size
                chunk_size = calc_chunk_size(sec_per_file)

                #pbar.update(n=limit-remaining)
                pbar.update(n=chunk_limit)
        
        if print_report:
            num_chunks = len(report)
            total_ins  = sum(map(lambda c:c['chunk_size'], report))
            avg_chunk  = total_ins / num_chunks
            total_time = sum(map(lambda c:c['chunk_time_used'], report))
            avg_time   = total_time / num_chunks
            time_match = avg_time / max(chunk_time*100, 1)

            print( 'Sync report:                                              ')
            print(f'    Total chunks         : {num_chunks}                   ')
            print(f'    Total file inserts   : {total_ins} / {limit}          ')
            print(f'    Average chunk size   : {avg_chunk:.2f}                ')
            print(f'    Total time spent     : {total_time:.2f}s              ')
            print(f'    Average time / chunk : {avg_time:.2f}s / {chunk_time}s')
            print(f'    Percent time match   : {time_match:.2f}%              ')
            
        if dry: return dry_inserts

    def migrate(self):
        # create MD object representation current DB state
        pre_metadata = sa.MetaData()
        pre_metadata.reflect(bind=self.engine)

        table_rows = {}
        for table_name, table in pre_metadata.tables.items():
            # pre-migration rows
            select_results = utils.db.sa_execute(self.engine, sa.select(table))
            named_results  = utils.db.named_results(table, select_results)

            table_rows[table_name] = named_results


        #table_update_map = {
        #    'files': (
        #        lambda r: File(Path(NOTEPATH, r['name']), NOTEPATH).get_table_data()
        #    ),
        #    'notes': (
        #        lambda r: Note(Path(NOTEPATH, r['name']), NOTEPATH).get_table_data()
        #    ),
        #    'note_conversion_matter': (
        #        lambda r: Note(Path(NOTEPATH, r['name']), NOTEPATH).get_table_data()
        #    ),
        #}

        # update schema
        self.recreate()

        with self.engine.connect() as connection:
            for table_name, table in tables.metadata.tables.items():
                if table_name not in table_rows: continue

                logger.info(f'Migrating table "{table_name}"')

                if table_name == 'files':
                    update_note_rows = []

                    for i,note_row in enumerate(table_rows['files']):
                        file = File(Path(NOTEPATH, note_row['name']), NOTEPATH)
                        u_note_row = utils.db.prepare_insert(
                            tables.files,
                            file.get_table_data()
                        )
                        note_row.update({k:v for k,v in u_note_row.items() if v})
                        update_note_rows.append(note_row)

                    table_rows['files'] = update_note_rows
                elif table_name == 'notes':
                    update_note_rows = []

                    for note_row in table_rows['notes']:
                        note = Note(Path(NOTEPATH, note_row['name']), NOTEPATH)
                        u_note_row = utils.db.prepare_insert(
                            tables.notes,
                            note.get_table_data()
                        )
                        note_row.update({k:v for k,v in u_note_row.items() if v})
                        update_note_rows.append(note_row)

                    table_rows['notes'] = update_note_rows

                # generic update blueprint for later; now doesn't matter as conversion based
                # inserts require re-converting...so you may as well just do a full re-build.
                #update_rows = []
                #if table_name in table_update_map:
                #    for i,row in enumerate(table_rows['files']):
                #        update_obj = table_update_map[table_name](row)

                #        u_row = utils.db.prepare_insert(
                #            table,
                #            note.get_table_data()
                #        )
                #        note_row.update({k:v for k,v in u_note_row.items() if v})
                #        update_note_rows.append(note_row)

                connection.execute(
                    sa.insert(table),
                    table_rows[table_name]
                )

            connection.commit()


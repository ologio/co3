'''
.. admonition:: Common on insert behavior

    - Tables with unique constraints have been equipped with ``sqlite_on_conflict_unique``
      flags, enabling conflicting bulk inserts to replace conflicting rows gracefully. No
      need to worry about explicitly handling upserts.
    - The bulk insert via conn.execute(<insert>,<row_list>) automatically ignores
      irrelevant column names within provided record dicts, whereas explicit .values() calls
      for non-bulk inserts will throw errors if not aligned perfectly. We want to keep these
      bulk calls AND allow update/replacements on conflict; the setting above allows this
      bulk usage to continue as is.

.. admonition:: Options for insert/update model

    1. Collect all possible update objects, and use a SQLite bulk insert that only updates
       if needed (based on modified time, for example). This sounds on the surface like
       the right approach since we defer as much to bulk SQL logic, but it's far and away
       the worse option b/c prepping all file/note/etc objects is too expensive to
       ultimately throw away when finding out an update isn't needed. For example, if we
       wanted to perform note updates _inside_ the SQL call (like an ``INSERT .. UPDATE ..
       IF``, as opposed to determining the objects to insert _outside_ of the SQL call),
       you would need to bring each of the note's HTML prior to the insert check. There's
       no 2-stage processing here where you can check if the note needs to be converted
       b/c it's out of date, and only then perform the computation.
    2. Instantiate objects sequentially, each time checking with the DB to see if full
       processing is needed. This makes much more sense when the full processing is very
       expensive, as it is with Note conversion. This would iterate through available notes,
       perform a ``SELECT`` on the target table to see if the note needs updating, and if so
       perform the remaining computation. Those objects then get added to a "update object
       list" to be inserted in bulk, but you make sequential ``SELECT`` checks before that.
    
       The one extra optimization you could maybe make here is doing a full SELECT on the
       target table and bring all rows into memory before iterating through the objects.
       This would likely make it faster than whatever SQLAlchemy overhead there may be. It
       also might just be outright required given Connection objects aren't thread-safe;
       we can get away with single thread global SELECT, threaded checking during object
       build, then single thread bulk INSERT. (**Note**: this is what the method does).
'''

import time
import logging
import threading
from pathlib import Path
from concurrent.futures import wait, as_completed

import sqlalchemy as sa
from tqdm.auto import tqdm

from co3 import util
from co3.schema import Schema
from co3.engines import SQLEngine
from co3.manager import Manager
from co3.components import Relation, SQLTable


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
    Manager (see the Collector). We do, however, implement a ``sync`` operation that can
    saturates a router with events (dynamically) and sweeps up inserts on session basis
    from an attached collector.
    '''
    def __init__(self, *args, **kwargs):
        '''
        The insert lock is a *reentrant lock*, meaning the same thread can acquire the
        lock again without deadlocking (simplifying across methods of this class that
        need it).
        '''
        super().__init__(*args, **kwargs)

        self._insert_lock = threading.RLock()

    def update(self):
        pass

    def migrate(self):
        pass

    def sync(self):
        pass

    def recreate(
        self,
        schema: Schema[SQLTable],
        engine: SQLEngine
    ) -> None:
        '''
        Ideally this remains open, as we can't necessarily rely on a SQLAlchemy metadata
        object for all kinds of SQLDatabases (would depend on the backend, for instance). 

        Haven't quite nailed down how backend instances should be determined; something
        like SQLAlchemySQLManager doesn't seem great. Nevertheless, this method likely
        cannot be generalized at the "SQL" (general) level.
        '''
        metadata = next(iter(schema._component_set)).obj.metadata
        metadata.drop_all(engine.manager)
        metadata.create_all(engine.manager, checkfirst=True)

    def insert(
        self,
        connection,
        component,
        inserts: list[dict],
        commit=True
    ):
        '''
        Parameters:
        '''
        with self._insert_lock:
            res = connection.execute(
                sa.insert(component.obj),
                inserts
            )

            if commit:
                connection.commit()

        return res

    def insert_many(self, connection, inserts: dict):
        '''
        Perform provided table inserts, aligning the insert format of
        ``Collector.collect_inserts()``.

        Parameters:
            inserts: component-indexed dictionary of insert lists
        '''
        total_inserts = sum([len(ilist) for ilist in inserts.values()])
        if total_inserts < 1: return

        logger.info(f'Total of {total_inserts} sync inserts to perform')
        start = time.time()

        # TODO: add some exception handling? may be fine w default propagation
        res_list = []
        with self._insert_lock:
            for component in inserts:
                comp_inserts = inserts[component]
                if len(comp_inserts) == 0: continue

                logger.info(
                    f'Inserting {len(comp_inserts)} out-of-date entries into component "{component}"'
                )

                res = self.insert(connection, component, comp_inserts, commit=False)
                res_list.append(res)

            connection.commit()
            logger.info(f'Insert transaction completed successfully in {time.time()-start:.2f}s')

        return res_list


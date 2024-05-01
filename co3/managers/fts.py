import time
import logging
from collections import defaultdict

from tqdm import tqdm
import sqlalchemy as sa

from co3 import util
from co3.manager import Manager
from co3.accessors.sql import SQLAccessor


logger = logging.getLogger(__name__)

class FTSManager(Manager):
    def __init__(self):
        self.sql_accessor = SQLAccessor()

    def recreate_from_table(self, table: str, cols):
        inserts, res_cols = self.accessor.select(
            table,
            cols=cols,
            include_cols=True,
        )

        util.db.populate_fts5(
            self.engine,
            ['search'],
            columns=res_cols,
            inserts=inserts,
        )

    def recreate(self):
        logger.info(f'FTS recreate: insert post-processing took {time.time()-start:.2f}s')

        util.db.populate_fts5(
            self.engine,
            ['search'],
            columns=list(cols),
            inserts=inserts,
        )

    def update(self): pass

    def sync(self): pass

    def migrate(self): pass


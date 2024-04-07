'''
Defines the Collector base class.

This module is the critical "middleware" connecting the primitive object definitions and
their representations in the database. It operates with full knowledge of how both are
defined, and abstracts away both the prep work for DB insertions as well as updates
trickling down the primitive hierarchy.

The `src` format target is re-used for both canonical tables/primitives, as well as
<prim>_conversion_matter tables in tables/conversions under the `src` format. The latter
is meant to extend those attributes that are format-specific (i.e., would change when, say,
converting to `html5`), and thus need to be broken across the format dimension.

Note:
    Despite the structure of the database module, this class does not currently inherit
    from a super class in localsys.db (like the accessors and managers, for instance).
    This will likely ultimately be the model that's embraced, but until FTS (or other
    groups) need a collector, this will be remain an independent class. It is, however,
    named like a concrete subclass, taking on the "Core" prefix.
'''

from pathlib import Path
from collections import defaultdict
import logging
import importlib
import subprocess
from uuid import uuid4

import sqlalchemy as sa

from co3 import util
from co3.component import Component
#from localsys.db.schema import tables


logger = logging.getLogger(__name__)

class Collector[C: Component, M: 'Mapper[C]']:
    def __init__(self):
        self._inserts = defaultdict(lambda: defaultdict(list))

    @property
    def inserts(self):
        return self._inserts_from_receipts()

    def _inserts_from_receipts(self, receipts=None, pop=False):
        inserts = defaultdict(list)
        
        if receipts is None:
            receipts = list(self._inserts.keys())

        for receipt in receipts:
            if pop: insert_dict = self._inserts.pop(receipt, {})
            else:   insert_dict = self._inserts[receipt]

            for table, insert_list in insert_dict.items():
                inserts[table].extend(insert_list)

        return dict(inserts)

    def _reset_session(self):
        self._inserts = defaultdict(lambda: defaultdict(list))

    def _generate_unique_receipt(self):
        return str(uuid4())

    def add_insert(self, table_name, insert_dict, receipts=None):
        '''
        TODO: formalize table_name mapping; at class level provide a `table_map`, or provide
        the table object itself to this method
        '''
        if table_name not in tables.table_map:
            #logger.debug(f'Inserts provided for non-existent table {table_name}')
            return None

        receipt = self._generate_unique_receipt()

        self._inserts[receipt][table_name].append(
            utils.db.prepare_insert(
                tables.table_map[table_name], 
                insert_dict
            )
        )

        if receipts is not None:
            receipts.append(receipt)

        return receipt

    def collect_inserts(self, receipts=None):
        '''
        Collect insert-ready dictionaries for the core primitive schema. This method is
        effectively a light wrapper around the File and Note-based collection logic
        elsewhere in the class. 

        The overall collection scheme embraces a session-like sequential update model to
        an internal insert tracker. The sequence of insert methods is ordered according to
        the schema hierarchy, and higher level inserts dictate the scope for lower level
        inserts (all methods check and populate the same `inserts` dictionary). Calling
        this method flushes any existing inserts, ensuring a re-scan takes place across
        calls (or "sessions").

        Parameters:
            skip_updated: whether to ignore primitives with existing up-to-date
                          database entries

        Returns:
            Table name-indexed dictionary of insert lists (of column name-indexed dicts)
        '''
        return self._inserts_from_receipts(receipts, pop=True)

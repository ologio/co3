'''
Database

Central object for defining storage protocol-specific interfaces. The database wraps up
central items for interacting with database resources, namely the Accessor and Manager
objects.

The Database type hierarchy attempts to be exceedingly general; SQL-derivatives should
subclass from the RelationalDatabase subtype, for example, which itself becomes a new
generic via type dependence on Relation.
'''

import logging
from typing import Self

from co3.accessor import Accessor
from co3.composer import Composer
from co3.manager  import Manager
from co3.indexer  import Indexer

logger = logging.getLogger(__name__)


class Database[C: Component]:
    accessor: type[Accessor[C, Self]] = Accessor[C, Self]
    manager:  type[Manager[C, Self]]  = Manager[C, Self]

    def __init__(self, resource):
        '''
        Variables:
            _local_cache: a database-local property store for ad-hoc CacheBlock-esque
                          methods, that are nevertheless _not_ query/group-by responses to
                          pass on to the Indexer. Dependent properties should write to the
                          this cache and check for existence of stored results; the cache
                          state must be managed globally.
        ''' 
        self.resource = resource

        self._access = self.accessor(self)
        self._manage = self.manager(self)

        self._index  = Indexer(self._access)
        self._local_cache = {}

        self.reset_cache = False

    @property
    def engine(self):
        '''
        Database property to provide a singleton engine for DB interaction, initializing
        the database if it doesn't already exist.

        TODO: figure out thread safety across engines and/or connection. Any issue with
        hanging on to the same engine instance for the Database instance?
        '''
        raise NotImplementedError

    def connect(self):
        self.engine.connect()

    @property
    def access(self):
        return self._access

    @property
    def compose(self):
        return self._compose

    @property
    def index(self):
        if self.reset_cache:
            self._index.cache_clear()
            self.reset_cache = False
        return self._index

    @property
    def manage(self):
        '''
        Accessing `.manage` queues a cache clear on the external index, as well wipes the
        local index.
        '''
        self.reset_cache = True
        self._local_cache = {}
        return self._manage

    def populate_indexes(self): pass


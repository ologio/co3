'''
Manager

Wrapper for state-changing database operations. Managers expose the formalized means of
interacting with an underlying database, like inserts and schema recreation.
'''
from pathlib import Path
from abc import ABCMeta, abstractmethod

from co3.schema import Schema
#from co3.database import Database


class Manager[C: Component, D: 'Database[C]'](metaclass=ABCMeta):
    '''
    Management wrapper class for table groupings.

    The schema is centered around a connected group of tables (via foreign keys). Thus,
    most operations need to be coordinated across tables. A few common operations are
    wrapped up in this class to then also be mirrored for the FTS counterparts.
    '''
    def __init__(self, database: D):
        self.database = database

    @abstractmethod
    def recreate(self, schema: Schema[C]):
        raise NotImplementedError

    @abstractmethod
    def migrate(self):
        raise NotImplementedError

    @abstractmethod
    def insert(self):
        raise NotImplementedError

    @abstractmethod
    def sync(self):
        raise NotImplementedError

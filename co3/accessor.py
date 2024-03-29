'''
Accessor

Provides access to an underlying schema through a supported set of operations. Class
methods could be general, high-level SQL wrappers, or convenience functions for common
schema-specific queries.
'''
import inspect
from pathlib import Path
from collections import defaultdict

import sqlalchemy as sa

#from co3.database import Database


class Accessor[D: 'Database']:
    '''
    Access wrapper class for complex queries and easy integration with Composer tables.
    Implements high-level access to things like common constrained SELECT queries.

    Parameters:
        engine: SQLAlchemy engine to use for queries. Engine is initialized dynamically as
                a property (based on the config) if not provided
    '''
    def __init__(self, database: D):
        self.database = database


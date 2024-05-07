'''
Provides access to an underlying schema through a supported set of operations. Class
methods could be general, high-level SQL wrappers, or convenience functions for common
schema-specific queries.
'''
import time
import inspect
from pathlib import Path
from collections import defaultdict
from abc import ABCMeta, abstractmethod

import sqlalchemy as sa

from co3.component import Component


class Accessor[C: Component](metaclass=ABCMeta):
    '''
    Access wrapper class for complex queries and easy integration with Composer tables.
    Implements high-level access to things like common constrained SELECT queries.

    Instance variables:
        access_log: time-indexed log of access queries performed
    '''
    def __init__(self):
        self.access_log = {}

    def log_access(self, stmt):
        self.access_log[time.time()] = f'{stmt}'

    @abstractmethod
    def raw_select(
        self,
        connection,
        text: str,
    ):
        raise NotImplementedError

    @abstractmethod
    def select(
        self,
        connection,
        component: str | C,
        *args,
        **kwargs
    ):
        raise NotImplementedError

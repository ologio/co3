'''
Composer

Base for manually defining table compositions outside those natural to the schema
hierarchy (i.e., constructable by a `CO4.compose()` call).

Example: suppose we have a simple object hierarchy A(CO4) -> B -> C. C's in-built
`compose()` method may not always be desirable when constructing composite tables and
running related queries. In this case, a custom Composer can be used to make needed
composite tables easier to reference; in the case below, we define the "BC" composite
table.

```
class ExampleComposer(Composer):

    @register_table
    def BC(self):
        full_B = B.compose(full=True)
        full_C = C.compose(full=True)
        
        return full_B.join(
            full_C,
            full_B.c.name == full_C.c.name, # TODO: is this fine? or do we need base table refs
            outer=True
        )
'''
from pathlib import Path

from co3.mapper import Mapper


def register_table(table_name=None):
    '''
    Registry decorator for defined composer classes. Decorating a class method simply
    attaches a `table_name` attribute to it, setting it to either a provided value or the
    name of the method itself. Methods with a `table_name` attribute are later swept up at
    the class level and placed in the `table_map`.
    '''
    def decorator(func):
        if table_name is None:
            table_name = func.__name__
        func.table_name = table_name
        return func
    return decorator

class Composer[M: Mapper]:
    '''
    Base composer wrapper for table groupings.

    The schema is centered around a connected group of tables (via foreign keys). Thus,
    most operations need to be coordinated across tables. The `accessors` submodules
    are mostly intended to provide a "secondary layer" over the base set of tables in the
    schema, exposing common higher level table compositions (i.e., chained JOINs). See
    concrete instances (e.g., CoreAccess, FTSAccessor) for actual implementations these
    tables; the base class does not expose

    Tables in subclasses are registered with the `register_table` decorator, automatically
    indexing them under the provided name and making them available via the `table_map`.
    '''
    def __init__(self):
        self._set_tables()

    def _set_tables(self):
        '''
        Skip properties (so appropriate delays can be used), and 

        Set the table registry at the class level. This only takes place during the first
        instantiation of the class, and makes it possible to definitively tie methods to
        composed tables during lookup with `get_table()`.
        '''
        cls = self.__class__

        # in case the class has already be instantiated
        if hasattr(cls, 'table_map'): return

        table_map = {}
        for key, value in cls.__dict__.items():
            if isinstance(value, property):
                continue  # Skip properties
            if callable(value) and hasattr(value, 'table_name'):
                table_map[value.table_name] = value(self)

        cls.table_map = table_map

    def get_table(self, table_name):
        '''
        Retrieve the named table composition, if defined.
        '''
        return self.table_map.get(table_name)

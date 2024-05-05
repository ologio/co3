'''
Dev note:
    Any reason to have ComposeableComponents and Relations as separate types? The thought
    is that there may be some possible Component types we want to be able to Compose that
    wouldn't logically be Relations. But the gap here might be quite small
'''

from typing import Self
from abc import ABCMeta, abstractmethod

import sqlalchemy as sa

from co3.util.types import SQLTableLike
from co3.component import Component


class ComposableComponent[T](Component[T], metaclass=ABCMeta):
    '''
    Components that can be composed with others of the same type.
    '''
    @abstractmethod
    def compose(self, component: Self, on, outer=False) -> Self:
        '''
        Abstract composition.
        '''
        raise NotImplementedError


# relational databases
class Relation[T](ComposableComponent[T]):
    '''
    Relation base for tabular components to be used in relation DB settings. Attempts to
    adhere to the set-theoretic base outlined in the relational model [1]. Some
    terminology:

    Relation: table-like container
    | -> Heading: set of attributes
    |    | -> Attribute: column name
    | -> Body: set of tuples with domain matching the heading
    |    | -> Tuple: collection of values


    [1]: https://en.wikipedia.org/wiki/Relational_model#Set-theoretic_formulation

    Note: development tasks
        As it stands, the Relation skeleton is incredibly lax compared to the properties and
        operations that should be formally available, according its pure relational algebra
        analog. 

        Relations are also generic up to a type T, which ultimately serves as the base object
        for Relation instances. We aren't attempting to implement some generally useful
        table-like class here; instead we're just exposing a lightweight interface that's
        needed for a few CO3 contexts, and commonly off-loading most of the heavy-lifting to
        true relation objects like SQLAlchemy tables.
    '''
    def compose(
        self,
        _with: Self,
        on,
        outer=False
    ):
        return self

class SQLTable(Relation[SQLTableLike]):
    @classmethod
    def from_table(cls, table: sa.Table):
        '''
        Note that the sa.Table type is intentional here; not all matching types for
        SQLTableLike have a defined ``name`` property
        '''
        return cls(table.name, table)

    def get_attributes(self) -> tuple:
        return tuple(self.obj.columns)

    def get_column_defaults(self, include_all=True):
        '''
        Provide column:default pairs for a provided SQLAlchemy table.

        Parameters:
            include_all: whether to include all columns, even those without explicit defaults
        '''
        default_values = {}
        for column in self.get_attributes():
            if column.default is not None:
                default_values[column.name] = column.default.arg
            elif column.nullable:
                default_values[column.name] = None
            else:
                # assume empty string if include_all and col has no explicit default 
                # and isn't nullable
                if include_all and column.name != 'id':
                    default_values[column.name] = ''

        return default_values

    def prepare_insert_data(self, insert_data: dict) -> dict:
        '''
        Modifies insert dictionary with full table column defaults
        '''
        insert_dict = self.get_column_defaults()
        insert_dict.update(
            { k:v for k,v in insert_data.items() if k in insert_dict }
        )

        return insert_dict

    def compose(self, _with: Self, on, outer=False):
        return self.__class__(
            f'<{self.name}>+<{_with.name}>',
            self.obj.join(_with.obj, on, isouter=outer)
        )

class FTSTable(Relation[SQLTableLike]):
    pass

# key-value stores
class Dictionary(Relation[dict]):
    def get_attributes(self):
        return tuple(self.obj.keys())


# document databases
class Document[T](Component[T]):
    pass


# graph databases
class Node[T](Component[T]):
    pass

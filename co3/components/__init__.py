from typing import Self
from abc import ABCMeta, abstractmethod

import sqlalchemy as sa

from co3.util.types import TableLike
from co3.component import Component


class ComposableComponent[T](Component[T], metaclass=ABCMeta):
    '''
    Components that can be composed with others of the same type.
    '''
    @abstractmethod
    def compose(self, component: Self, on) -> Self:
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

class SQLTable(Relation[TableLike]):
    @classmethod
    def from_table(cls, table: sa.Table, schema: 'SQLSchema'):
        return cls(table.name, table, schema)

    def get_attributes(self):
        return tuple(self.obj.columns)


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

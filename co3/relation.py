'''
Relation

Loose wrapper for table-like objects to be used in various database contexts. Relations
can be thought of generally as named data containers that contain tuples of attributes
(adhering to relational algebra terms). Relation subtypes are referred to commonly across
CO3 generics, serving as a fundamental abstraction within particular storage protocols.

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
from typing import Self


class Relation[T]:
    def __init__(self, name, obj: T):
        self.name = name
        self.obj  = obj

    def get_attributes(self):
        raise NotImplementedError

    def join(
        self,
        corelation: Self,
        on,
        outer=False
    ):
        raise NotImplementedError

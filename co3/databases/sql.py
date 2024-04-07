from typing import Self

from co3.database import Database

from co3.accessors.sql import RelationalAccessor, SQLAccessor
from co3.managers.sql  import RelationalManager,  SQLManager

from co3.components import Relation, SQLTable


class RelationalDatabase[R: Relation](Database):
    '''
    accessor/manager assignments satisfy supertype's type settings;
    `TabluarAccessor[Self, R]` is of type `type[RelationalAccessor[Self, R]]`
    (and yes, `type[]` specifies that the variable is itself being set to a type or a
    class, rather than a satisfying _instance_)
    '''
    accessor: type[RelationalAccessor[Self, R]] = RelationalAccessor[Self, R]
    manager:  type[RelationalManager[Self, R]]  = RelationalManager[Self, R]


class SQLDatabase[R: SQLTable](RelationalDatabase[R]):
    accessor = SQLAccessor
    manager  = SQLManager


class SQLiteDatabase(SQLDatabase[SQLTable]):
    pass

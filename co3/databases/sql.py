from typing import Self

from co3.database import Database

from co3.accessors.sql import RelationalAccessor, TabularAccessor, SQLAccessor
from co3.managers.sql  import RelationalManager,  TabularManager,  SQLManager

from co3.relation import Relation
from co3.relations import TabularRelation, SQLTable


class RelationalDatabase[R: Relation](Database):
    accessor: type[RelationalAccessor[Self, R]] = RelationalAccessor[Self, R]
    manager:  type[RelationalManager[Self, R]]  = RelationalManager[Self, R]


class TabularDatabase[R: TabularRelation](RelationalDatabase[R]):
    '''
    accessor/manager assignments satisfy supertype's type settings;
    `TabluarAccessor[Self, R]` is of type `type[RelationalAccessor[Self, R]]`
    (and yes, `type[]` specifies that the variable is itself being set to a type or a
    class, rather than a satisfying _instance_)
    '''
    accessor: type[TabularAccessor[Self, R]] = TabularAccessor[Self, R]
    manager:  type[TabularManager[Self, R]]  = TabularManager[Self, R]


class SQLDatabase[R: SQLTable](TabularDatabase[R]):
    accessor = SQLAccessor
    manager  = SQLManager


class SQLiteDatabase(SQLDatabase[SQLTable]):
    pass

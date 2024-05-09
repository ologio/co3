import sqlalchemy as sa

from co3.database import Database, Engine

from co3.accessors.sql import RelationalAccessor, SQLAccessor
from co3.managers.sql  import RelationalManager,  SQLManager

from co3.engines import SQLEngine
from co3.components import Relation, SQLTable


class RelationalDatabase[C: Relation](Database[C]):
    '''
    accessor/manager assignments satisfy supertype's type settings;
    ``TabluarAccessor[Self, C]`` is of type ``type[RelationalAccessor[Self, C]]``
    (and yes, ``type[]`` specifies that the variable is itself being set to a type or a
    class, rather than a satisfying _instance_)
    '''
    _accessor_cls: type[RelationalAccessor[C]] = RelationalAccessor[C]
    _manager_cls:  type[RelationalManager[C]]  = RelationalManager[C]


class SQLDatabase[C: SQLTable](RelationalDatabase[C]):
    _accessor_cls = SQLAccessor
    _manager_cls  = SQLManager
    _engine_cls   = SQLEngine

    def raw_query(self, connection, query):
        return SQLEngine.execute(
            connection,
            sa.text(query)
        )
        

class SQLiteDatabase(SQLDatabase[SQLTable]):
    pass

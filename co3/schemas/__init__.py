from typing import Self

import sqlalchemy as sa

from co3.schema import Schema
from co3.components import Relation, SQLTable


class RelationalSchema[R: Relation](Schema[R]):
    pass

class SQLSchema(RelationalSchema[SQLTable]):
    @classmethod
    def from_metadata(cls, metadata: sa.MetaData):
        instance = cls()

        for table in metadata.tables.values():
            comp = SQLTable.from_table(table)
            instance.add_component(comp)

        return instance
        

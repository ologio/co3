import sqlalchemy as sa

from co3.relation import Relation


class DictRelation(Relation[dict]):
    def get_attributes(self):
        return tuple(self.obj.keys())

class TabularRelation(Relation[sa.Table]):
    def get_attributes(self):
        return tuple(self.obj.columns)


class SQLTable(Relation[sa.Table]):
    pass

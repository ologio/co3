'''
content -> converted
state   -> transformed
'''

from co3 import CO3, collate
from co3.databases import SQLiteDatabase
from co3 import Mapper


class Tomato(CO3):
    def __init__(self, size, ripe):
        self.size = size
        self.ripe = ripe

    @property
    def attributes(self):
        return vars(self)

    @collate('diced')
    def dice(self):
        return self.size / 2

    @collate('roasted')
    def roast(self):
        return self.size / 2


metadata = sa.MetaData()
tomato_table = sa.Table(
    'tomato',
    metadata,
    sa.Column('id', sa.Integer, primary_key=True),
)
tomato_schema = Schema.from_metadata(metadata)

mapper = Mapper()
mapper.attach(
    Tomato, 
    tomato_table
)


tomato = Tomato(5, False)
mapper.collect(tomato, for='diced')

db = SQLiteDatabse('resource.sqlite')
db.recreate(tomato_schema)

# for non-raw DB ops, consider requiring a verify step first. Keeps up integrity between
# runs
db.verify(tomato_schema)
# then
# not too straightforward, but can also verify mapper compositions if they use a schema
# that's been verified by the database

db.sync(mapper)

dict_results = db.select(
    mapper.compose(Tomato),
    tomato_table.c.size == 5
)

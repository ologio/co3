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


tomato_table = sa.Table()

mapper = Mapper()
mapper.attach(
    Tomato, 
    tomato_table
)


tomato = Tomato(5, False)
mapper.collect(tomato, for='diced')

db = SQLiteDatabse('resource.sqlite')
db.sync(mapper)

dict_results = db.select(
    mapper.compose(Tomato),
    tomato_table.c.size == 5
)

'''
just remembered tomatos aren't vegetables. whoops
'''
import random

import sqlalchemy as sa

from co3.schemas import SQLSchema
from co3 import CO3, collate, Mapper, ComposableMapper
from co3 import util


class Vegetable(CO3):
    def __init__(self, name, color):
        self.name = name
        self.color = color

    #@abstractmethod
    @collate
    def cut(self, method):
        raise NotImplementedError

class Tomato(Vegetable):
    def __init__(self, name, radius):
        super().__init__(name, 'red')
        self.radius = radius

    @property
    def attributes(self):
        return vars(self)
        
    def collation_attributes(self, key, group):
        return {
            'name': self.name,
            'state': key,
        }

    @collate('ripe', groups=['aging'])
    def ripen(self):
        return {
            'age': random.randint(1, 6)
        }

    @collate('rotten', groups=['aging'])
    def rot(self):
        return {
            'age': random.randint(4, 9)
        }
        
    @collate('diced', groups=['cooking'])
    def dice(self):
        return {
            'pieces': random.randint(2, 12)
        }

    @collate
    def cut(self, method):
        if method == 'slice':
            return {
                'pieces': random.randint(2, 5)
            }
        elif method == 'dice':
            return self.dice()


type_list = [Vegetable, Tomato]

'''
VEGETABLE
|
TOMATO -- AGING
       |
       -- COOKING

Note: foreign keys need to represent values that could be known by objects _without_ first interacting
with a DB. This is slightly non-standard, given how common it is to depend on another table's integer ID
(typically a value assigned by the DB using an autoincrement, for example, and not specified explicitly 
within the insertion body). As a result, SQLTable components need to be able to operate by another unique
key when expected to connect to other tables in the hierarchy. Below we use `name` with a UNIQUE constraint
for this purpose. Note that having an integer `id` is still perfectly okay so that a table can manage
uniqueness of its own rows by default.
'''
metadata = sa.MetaData()
vegetable_table = sa.Table(
    'vegetable',
    metadata,
    sa.Column('id',    sa.Integer, primary_key=True),
    sa.Column('name',  sa.String, unique=True),
    
    sa.Column('color', sa.String),
)
tomato_table = sa.Table(
    'tomato',
    metadata,
    sa.Column('id',   sa.Integer, primary_key=True),
    sa.Column('name', sa.String, util.db.deferred_cd_fkey('vegetable.name'), unique=True),
    
    sa.Column('radius', sa.Integer),
)
tomato_aging_table = sa.Table(
    'tomato_aging_states',
    metadata,
    sa.Column('id',   sa.Integer, primary_key=True),
    sa.Column('name', sa.String, util.db.deferred_cd_fkey('tomato.name'), unique=True),
    
    sa.Column('state', sa.String),
    sa.Column('age',   sa.Integer),
)
tomato_cooking_table = sa.Table(
    'tomato_cooking_states',
    metadata,
    sa.Column('id',   sa.Integer, primary_key=True),
    sa.Column('name', sa.String, util.db.deferred_cd_fkey('tomato.name'), unique=True),
    
    sa.Column('state',  sa.String),
    sa.Column('pieces', sa.Integer),
)
vegetable_schema = SQLSchema.from_metadata(metadata)

def general_compose_map(c1, c2):
    return c1.obj.c.name == c2.obj.c.name
    
vegetable_mapper = ComposableMapper(
    vegetable_schema,
    attr_compose_map=general_compose_map,
    coll_compose_map=general_compose_map,
)

def attr_name_map(cls):
    return f'{cls.__name__.lower()}'

def coll_name_map(cls, group):
    return f'{cls.__name__.lower()}_{group}_states'

vegetable_mapper.attach_many(
    type_list,
    attr_name_map,
    coll_name_map,
)

'''
new mapping type for Mapper attachment:

Callable[ [type[CO3], str|None], tuple[str, tuple[str], tuple[str]]]

tail tuples to associate column names from central table to collation

this should complete the auto-compose horizontally
'''


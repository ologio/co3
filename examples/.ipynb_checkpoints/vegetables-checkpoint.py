import random

import sqlalchemy as sa

from co3.schemas import SQLSchema
from co3 import CO3, collate
from co3 import util


class Vegetable(CO3):
    def __init__(self, name, color):
        self.name = name
        self.color = color

class Tomato(Vegetable):
    def __init__(self, name, radius):
        super().__init__(name, 'red')
        self.radius = radius

    @property
    def attributes(self):
        return vars(self)

    @collate('ripe', action_groups=['aging'])
    def ripen(self):
        return {
            'age': random.randint(1, 6)
        }

    @collate('rotten', action_groups=['aging'])
    def rot(self):
        return {
            'age': random.randint(4, 9)
        }
        
    @collate('diced', action_groups=['cooking'])
    def dice(self):
        return {
            'pieces': random.randint(2, 12)
        }

'''
VEGETABLE
|
TOMATO -- AGING
       |
       -- COOKING
'''
metadata = sa.MetaData()
vegetable_table = sa.Table(
    'vegetable',
    metadata,
    sa.Column('id',    sa.Integer, primary_key=True),
    sa.Column('name',  sa.String),
    sa.Column('color', sa.String),
)
tomato_table = sa.Table(
    'tomato',
    metadata,
    sa.Column('id',   sa.Integer, primary_key=True),
    sa.Column('vegetable_id',   sa.Integer, util.db.deferred_cd_fkey('vegetables.id')),
    sa.Column('radius', sa.Integer),
)
tomato_aging_table = sa.Table(
    'tomato_aging_states',
    metadata,
    sa.Column('id',   sa.Integer, primary_key=True),
    sa.Column('vegetable_id',   sa.Integer, util.db.deferred_cd_fkey('vegetables.id')),
    sa.Column('state', sa.String),
    sa.Column('age', sa.Integer),
)
tomato_cooking_table = sa.Table(
    'tomato_cooking_states',
    metadata,
    sa.Column('id',   sa.Integer, primary_key=True),
    sa.Column('vegetable_id',   sa.Integer, util.db.deferred_cd_fkey('vegetables.id')),
    sa.Column('state', sa.String),
    sa.Column('pieces', sa.Integer),
)
vegetable_schema = SQLSchema.from_metadata(metadata)
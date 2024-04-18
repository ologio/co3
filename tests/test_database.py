from co3.components import Relation
from co3.databases import SQLDatabase

from setups import vegetables as veg


db = None

def test_database_init():
    global db

    db = SQLDatabase('sqlite://')
    assert True
    
def test_database_recreate():
    db.recreate(veg.vegetable_schema)
    assert True

def test_database_insert():
    tomato = veg.Tomato('t1', 5)
    veg.vegetable_mapper.collect(tomato)

    # test instance as arg
    tom_comp = veg.vegetable_mapper.get_attr_comp(tomato)

    inserts = veg.vegetable_mapper.collector.collect_inserts()
    tom_inserts = inserts.get(tom_comp)
    assert tom_inserts is not None

    with db.engine.connect() as connection:
        assert db.manager.insert(
            connection,
            tom_comp,
            tom_inserts,
        ) is not None

def test_database_insert_many():
    tomato = veg.Tomato('t2', 5)
    veg.vegetable_mapper.collect(tomato)

    with db.engine.connect() as connection:
        assert db.manager.insert_many(
            connection,
            veg.vegetable_mapper.collector.inserts,
        ) is not None

def test_database_access():
    agg_table = veg.vegetable_mapper.compose(veg.Tomato)

    with db.engine.connect() as connection:
        assert db.accessor.select(
            connection,
            agg_table,
        ) is not None

from co3.components import Relation

from setups import vegetables as veg


def test_mapper_getters():
    veg_comp = veg.vegetable_schema.get_component('vegetable')
    tom_comp = veg.vegetable_schema.get_component('tomato')

    assert veg.vegetable_mapper.get_attribute_comp(veg.Vegetable) is veg_comp
    assert veg.vegetable_mapper.get_attribute_comp(veg.Tomato) is tom_comp

    tom_aging = veg.vegetable_schema.get_component('tomato_aging_states')
    tom_cooking = veg.vegetable_schema.get_component('tomato_cooking_states')

    assert veg.vegetable_mapper.get_collation_comp(veg.Tomato, 'aging') is tom_aging
    assert veg.vegetable_mapper.get_collation_comp(veg.Tomato, 'cooking') is tom_cooking

def test_mapper_attach():
    assert veg.vegetable_mapper.attach(
        veg.Tomato,
        'tomato',
        coll_groups={
            'aging':   'tomato_aging_states',
            'cooking': 'tomato_cooking_states',
        },
    ) is None

def test_mapper_attach_many():
    assert veg.vegetable_mapper.attach_many(
        [veg.Vegetable, veg.Tomato],
        lambda t: f'{t.__name__.lower()}'
    ) is None

def test_mapper_collect():
    tomato = veg.Tomato('t1', 10)
    receipts = veg.vegetable_mapper.collect(tomato)

    assert len(receipts) == 2

    # attempt to retrieve receipts one at a time
    res1 = veg.vegetable_mapper.collector.collect_inserts([receipts[0]])

    assert len(res1) == 1 # should be just one match
    assert len(res1[next(iter(res1.keys()))]) == 1 # and one dict for matching comp

    # try again, check no persistent match
    res1 = veg.vegetable_mapper.collector.collect_inserts([receipts[0]])

    assert len(res1) == 0 # should be no matches for the same receipt

    res2 = veg.vegetable_mapper.collector.collect_inserts([receipts[1]])

    assert len(res2) == 1
    assert len(res2[next(iter(res2.keys()))]) == 1



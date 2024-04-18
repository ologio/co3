from co3.components import Relation

from setups import vegetables as veg


def test_schema_get():
    veg_comp_raw = veg.vegetable_schema._component_map.get('vegetable')
    veg_comp     = veg.vegetable_schema.get_component('vegetable')

    assert veg_comp_raw is veg_comp

def test_schema_contains():
    vegetable_comp    = veg.vegetable_schema.get_component('vegetable')
    tomato_comp       = veg.vegetable_schema.get_component('tomato')
    tomato_aging_comp = veg.vegetable_schema.get_component('tomato_aging_states')

    assert vegetable_comp in veg.vegetable_schema
    assert tomato_comp in veg.vegetable_schema
    assert tomato_aging_comp in veg.vegetable_schema

def test_schema_add():
    veg.vegetable_schema.add_component(Relation[int]('a', 1))
    veg.vegetable_schema.add_component(Relation[int]('b', 2))

    assert veg.vegetable_schema.get_component('a') is not None
    assert veg.vegetable_schema.get_component('b') is not None

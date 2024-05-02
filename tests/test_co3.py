from collections import defaultdict

from co3.components import Relation

from setups import vegetables as veg


tomato = veg.Tomato('t1', 10)

def test_co3_registry():
    keys_to_groups = defaultdict(list)

    # collect groups each key is associated
    for group, keys in tomato.group_registry.items():
        for key in keys:
            keys_to_groups[key].append(group)

    assert set(tomato.key_registry.get(None,{}).keys()) == set(keys_to_groups.get(None,[]))

    # check against `registry`, should map keys to all groups
    for key, group_dict in tomato.key_registry.items():
        assert keys_to_groups.get(key) == list(group_dict.keys())

def test_co3_attributes():
    assert tomato.attributes is not None

def test_co3_components():
    assert tomato.components is not None

def test_co3_collation_attributes():
    for group, keys in tomato.group_registry.items():
        for key in keys:
            assert tomato.collation_attributes(key, group) is not None

def test_co3_collate():
    for group, keys in tomato.group_registry.items():
        for key in keys:
            if key is None: continue
            assert tomato.collate(key, group=group) is not None

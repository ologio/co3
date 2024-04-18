from collections import defaultdict

from co3.components import Relation

from setups import vegetables as veg


tomato = veg.Tomato('t1', 10)

def test_co3_registry():
    keys_to_groups = defaultdict(list)

    # collect groups each key is associated
    for action_group, action_keys in tomato.group_registry.items():
        for action_key in action_keys:
            keys_to_groups[action_key].append(action_group)

    # check against `action_registry`, should map keys to all groups
    for action_key, (_, action_groups) in tomato.action_registry.items():
        assert keys_to_groups.get(action_key) == action_groups

def test_co3_attributes():
    assert tomato.attributes is not None

def test_co3_components():
    assert tomato.components is not None

def test_co3_collation_attributes():
    for action_group, action_keys in tomato.group_registry.items():
        for action_key in action_keys:
            assert tomato.collation_attributes(action_key, action_group) is not None

def test_co3_collate():
    for action_group, action_keys in tomato.group_registry.items():
        for action_key in action_keys:
            assert tomato.collate(action_key) is not None

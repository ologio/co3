from typing import TypeVar
from collections import namedtuple
from dataclasses import is_dataclass, asdict

import sqlalchemy as sa

# custom types
SQLTableLike = TypeVar('SQLTableLike', bound=sa.Table | sa.Subquery | sa.Join)


# type checking/conversion methods
def is_dataclass_instance(obj) -> bool:
    return is_dataclass(obj) and not isinstance(obj, type)

def is_namedtuple_instance(obj) -> bool:
    return (
        isinstance(obj, tuple) and
        hasattr(obj, '_asdict') and
        hasattr(obj, '_fields')
    )

def is_dictlike(obj) -> bool:
    if isinstance(obj, dict):
        return True
    elif is_dataclass_instance(obj):
        return True
    elif is_namedtuple_instance(obj):
        return True

    return False

def dictlike_to_dict(obj) -> dict:
    '''
    Attempt to convert provided object to dict. Will return dict no matter what, including
    an empty dict if not dict-like. Consider using ``is_dictlike`` to determine if this
    method should be called.
    '''
    if isinstance(obj, dict):
        return obj
    elif is_dataclass_instance(obj):
        return asdict(obj)
    elif is_namedtuple_instance(obj):
        return obj._asdict()

    return {}

'''
CO4

CO4 is an abstract base class for scaffolding object hierarchies and managing operations
with associated database schemas. It facilitates something like a "lightweight ORM" for
classes/tables/states with fixed transformations of interest. The canonical use case is
managing hierarchical document relations, format conversions, and syntactical components.
'''

import inspect
import logging
from functools import wraps, partial

#from localsys.db.schema import tables

logger = logging.getLogger(__name__)


#def register_format(_format):
#    def decorator(func):
#        self.collate.format_map[_format] = func
#
#        @wraps(func)
#        def register(*args, **kwargs):
#            return func(*args, **kwargs)
#
#        return register
#    return decorator

def collate(action_key, action_groups=None):
    def decorator(func):
        nonlocal action_groups
        
        if action_groups is None:
            action_groups = [None]
        func._action_data = (action_key, action_groups)
        return func
    return decorator

class FormatRegistryMeta(type):
    def __new__(cls, name, bases, attrs):
        action_registry = {}

        # add registered superclass methods; iterate over bases (usually just one), then
        # that base's chain down (reversed), then methods from each subclass
        for base in bases:
            for _class in reversed(base.mro()):
                methods = inspect.getmembers(_class, predicate=inspect.isfunction)
                for _, method in methods:
                    if hasattr(method, '_action_data'):
                        action_key, action_groups = method._action_data
                        action_registry[action_key] = (method, action_groups)

        # add final registered formats for the current class, overwriting any found in
        # superclass chain
        for attr_name, attr_value in attrs.items():
            if hasattr(attr_value, '_action_data'):
                action_key, action_groups = attr_value._action_data
                action_registry[action_key] = (method, action_groups)

        attrs['action_map'] = action_registry

        return super().__new__(cls, name, bases, attrs)

class CO3(metaclass=FormatRegistryMeta):
    '''
    CO3: COllate, COllect, COmpose - conversion & DB insertion base

    - Collate: organize and transform conversion outputs, possibly across class components
    - Collect: gather core attributes, conversion data, and subcomponents for DB insertion
    - Compose: construct object-associated DB table references through the class hierarchy

    Note: on action groups
        Group keys are simply named collections to make it easy for storage components to
        be attached to action subsets. They do _not_ augment the action registration
        namespace, meaning the action key should still be unique; the group key is purely
        auxiliary.

        Action methods can also be attached to several groups, in case there is
        overlapping utility within or across schemas or storage media. In this case, it
        becomes particularly critical to ensure registered `collate` methods really are
        just "gathering results" from possibly heavy-duty operations, rather than
        performing them when called, so as to reduce wasted computation.
    '''
    @property
    def attributes(self):
        '''
        Method to define how a subtype's inserts should be handled under `collect` for
        canonical attributes, i.e., inserts to the type's table.
        '''
        return vars(self)

    @property
    def components(self):
        '''
        Method to define how a subtype's inserts should be handled under `collect` for
        constituent components that need handling.
        '''
        return []

    def collate(self, action_key, *action_args, **action_kwargs):
        if action_key not in self.action_map:
            logger.debug(f'Collation for {action_key} not supported')
            return None
        else:
            return self.action_map[action_key](self)



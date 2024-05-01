'''
CO3 is an abstract base class for scaffolding object hierarchies and managing operations
with associated database schemas. It facilitates something like a "lightweight ORM" for
classes/tables/states with fixed transformations of interest. The canonical use case is
managing hierarchical document relations, format conversions, and syntactical components.

Generic collation syntax:

.. code-block:: python

    class Type(CO3):

        @collate
        def group(self, key):
            # disambiguate key
            ...

        @collate('key', groups=['group1', 'group2'])
        def key(self):
            # key-specific logic
            ...
'''
import inspect
import logging
from collections import defaultdict
from functools import wraps, partial


logger = logging.getLogger(__name__)

def collate(key, groups=None):
    '''
    Collation decorator for CO3 subtype action registry.

    Dynamic decorator; can be used as ``collate`` without any arguments, or with all. In
    the former case, ``key`` will be a function, so we check for this.

    .. admonition:: Usage

        Collation registration is the process of exposing various actions for use in
        **hierarchical collection** (see ``Mapper.collect``). Collation *keys* are unique
        identifiers of a particular action that emits data. Keys can belong to an arbitrary
        number of *groups*, which serve as semantically meaningful collections of similar
        actions. Group assignment also determines the associated *collation component*
        to be used as a storage target; the results of actions $K_G$ belonging to group
        $G$ will all be stored in the attached $G$-component. Specification of key-group
        relations can be done in a few ways:

        - Explicit key-group specification: a specific key and associated groups can be
          provided as arguments to the decorator:

          .. code-block:: python

              @collate('key', groups=['group1', 'group2'])
              def _key(self):
                  # key-specific logic
                  ...

          The registry dictionaries will then have the following items:

          .. code-block:: python

              key_registry = {
                  ...,
                  'key': (_key, ['group1', 'group2']),
                  ...
              }
              group_registry = {
                  ...,
                  'group1': [..., 'key', ...],
                  'group2': [..., 'key', ...],
                  ...
              }

          If ``groups`` is left unspecified, the key will be attached to the default
          ``None`` group.

        - Implicit key-group association: in some cases, you may want to support an entire
          "action class," and associate any operations under the class to the same storage
          component. Here we still use the notion of connecting groups to components, but
          allow the key to be dynamically specified and passed through to the collation
          method:

          .. code-block:: python

              @collate
              def group(self, key):
                  # disambiguate key
                  ...
          
          and in the registries:

          .. code-block:: python

              key_registry = {
                  ...,
                  None: {..., 'group': group, ...},
                  ...
              }
              group_registry = {
                  ...,
                  'group': [..., None, ...],
                  ...
              }

          A few important notes:

          - Implicit key-group specifications attach the *group* to a single method,
            whereas in the explicit case, groups can be affiliated with many keys. When
            explicitly provided, only those exact key values are supported. But in the
            implicit case, *any* key is allowed; the group still remains a proxy for the
            entire action class, but without needing to map from specifically stored key
            values. That is, the utility of the group remains consistent across implicit
            and explicit cases, but stores the associations differently.
          - The ``None`` key, rather than point to a ``(<method>, <group-list>)`` tuple,
            instead points to a dictionary of ``group``-``method`` pairs. When attempting
            execute a key under a particular group, the group registry indicates
            whether the key is explicitly supported. If ``None`` is present for the group,
            then ``key_registry[None][<group-name>]`` can be used to recover the method
            implicitly affiliated with the key (along with any other key under the group).
          - When any method has been implicitly registered, *any* key (even when
            attempting to specify an explicit key) will match that group. This can
            effectively mean keys are not unique when an implicit group has been
            registered. There is a protection in place here, however; in methods like
            ``CO3.collate`` and ``Mapper.collect``, an implicit group must be directly
            named in order for a given key to be considered. That is, when attempting
            collation outside specific group context, provided keys will only be
            considered against explicitly registered keys.
    '''
    func = None
    if inspect.isfunction(key):
        func = key
        key = None
        groups = [func.__name__]

    if groups is None:
        groups = [None]

    def decorator(f):
        f._collation_data = (key, groups)
        return f

    if func is not None:
        return decorator(func)

    return decorator

class FormatRegistryMeta(type):
    '''
    Metaclass handling collation registry at the class level.
    '''
    def __new__(cls, name, bases, attrs):
        key_registry = {}
        group_registry = defaultdict(list)

        def register_action(method):
            nonlocal key_registry, group_registry

            if hasattr(method, '_collation_data'):
                key, groups = method._collation_data

                if key is None:
                    # only add a "None" entry if there is _some_ implicit group
                    if None not in key_registry:
                        key_registry[None] = {}

                    # only a single group possible here
                    key_registry[None][groups[0]] = method
                else:
                    key_registry[key] = (method, groups)

                for group in groups:
                    group_registry[group].append(key)

        # add registered superclass methods; iterate over bases (usually just one), then
        # that base's chain down (reversed), then methods from each subclass
        for base in bases:
            for _class in reversed(base.mro()):
                methods = inspect.getmembers(_class, predicate=inspect.isfunction)
                for _, method in methods:
                    register_action(method)

        # add final registered formats for the current class, overwriting any found in
        # superclass chain
        for attr_name, attr_value in attrs.items():
            register_action(attr_value)

        attrs['key_registry'] = key_registry
        attrs['group_registry'] = group_registry

        return super().__new__(cls, name, bases, attrs)

class CO3(metaclass=FormatRegistryMeta):
    '''
    Conversion & DB insertion base class

    CO3: COllate, COllect, COmpose

    - Collate: organize and transform conversion outputs, possibly across class components
    - Collect: gather core attributes, conversion data, and subcomponents for DB insertion
    - Compose: construct object-associated DB table references through the class hierarchy

    .. admonition:: on action groups

        Group keys are simply named collections to make it easy for storage components to
        be attached to action subsets. They do _not_ augment the action registration
        namespace, meaning the action key should still be unique; the group key is purely
        auxiliary.

        Action methods can also be attached to several groups, in case there is
        overlapping utility within or across schemas or storage media. In this case, it
        becomes particularly critical to ensure registered ``collate`` methods really are
        just "gathering results" from possibly heavy-duty operations, rather than
        performing them when called, so as to reduce wasted computation.
    '''
    @property
    def attributes(self):
        '''
        Method to define how a subtype's inserts should be handled under ``collect`` for
        canonical attributes, i.e., inserts to the type's table.
        '''
        return vars(self)

    @property
    def components(self):
        '''
        Method to define how a subtype's inserts should be handled under ``collect`` for
        constituent components that need handling.
        '''
        return []

    def collation_attributes(self, key, group):
        '''
        Return "connective" collation component data, possibly dependent on
        instance-specific attributes and the action arguments. This is typically the
        auxiliary structure that may be needed to attach to responses from registered
        ``collate`` calls to complete inserts.

        Note: this method is primarily used by ``Mapper.collect()``, and is called just
        prior to collector send-off for collation inserts and injected alongside collation
        data. Common structure in collation components can make this function easy to
        define, independent of action group for instance.
        '''
        return {}

    def collate(self, key, group=None, *args, **kwargs):
        if key is None:
            return None

        if key not in self.key_registry:
            # keys can't match implicit group if that group isn't explicitly provided
            if group is None:
                logger.debug(
                    f'Collation for "{key}" not supported, or implicit group not specified'
                )
                return None

            method = self.key_registry[None].get(group)
            if method is None:
                logger.debug(
                    f'Collation key "{key}" not registered and group {group} not implicit'
                )
                return None

            return method(self, key, *args, **kwargs)
        else:
            method = self.key_registry[key][0]
            return method(self, *args, **kwargs)



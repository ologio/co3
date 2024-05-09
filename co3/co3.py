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

.. admonition:: On multi-key attachment

    One possible quirk of the current collation registry scheme is the rather black and
    white nature of key attachment. You either specify a single key, possibly to several
    groups, or allow any key via passthrough under an implicit group. There's no explicit
    "multi-key" pattern to make use of here, be it through "restricted passthrough"
    (method still parameterized by the key, but only allows keys from a provided list) or
    just simple duplicated attachment. To demonstrate via the above example:

    .. code-block:: python

        class Type(CO3):

            @collate(['key1', 'key2'], groups=['group1', 'group2'])
            def keys(self, key):
                # accept key as arg, but can only be 'key1' or 'key2'
                ...

    This could be integrated straightforwardly in the existing registration handler, but
    for the time being, it muddies the waters too much for the convenience it provides.
    For starters, this isn't an all too common pattern, and you possibly open up a
    slippery slope of the allowed key spec for a given method (wildcards/regex patterns?
    combinatorial O(nm) key (n) group (m) pairs to register?). It can also be handled in a
    few very simple ways if needed, either via full passthrough with an internal check:

    .. code-block:: python

            @collate(groups=['group1', 'group2'])
            def keys(self, key):
                if key not in ['key1', 'key2']:
                    return None

                ...

    or with a central handler and separate collation points (at least when the key list is
    small):

    .. code-block:: python

            def _handle_supported_keys(self, key):
                # expects only supported keys, e.g., 'key1' and 'key2'
                ...
                
            @collate('key1')
            def key1(self):
                self._handle_supported_keys('key1')

            @collate('key2')
            def key2(self):
                self._handle_supported_keys('key2')

    The former scales better and allows general key rejection patterns if needed, while
    the latter integrates a bit better with the formal collation process, e.g., will
    throw ``ValueErrors`` based on key mismatches automatically.
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
        key_registry = defaultdict(dict)
        group_registry = defaultdict(set)

        def register_action(method):
            nonlocal key_registry, group_registry

            if hasattr(method, '_collation_data'):
                key, groups = method._collation_data
                for group in groups:
                    key_registry[key][group] = method
                    group_registry[group].add(key)

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
    Base class supporting the central "COllate, COllect, COmpose" paradigm.

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

    .. admonition:: New: collation caching

        To help facilitate the common pattern of storing collation results, a
        ``collate_cache`` parameter has been added to store key-group indexed collation
        results. (Note: now requires explicit superclass instantiation.)
    '''
    def __init__(self):
        self._collate_cache = {}

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

    def collate(
        self,
        key,
        group                = None,
        args   : list | None = None,
        kwargs : dict | None = None,
    ):
        '''
        Note:
            This method is sensitive to group specification. By default, the provided key
            will be checked against the default ``None`` group, even if that key is only
            attached to non-default groups. Collation actions are unique on key-group
            pairs, so more specificity is generally required to correctly execute desired
            actions (otherwise, rely more heavily on the default group).
        '''
        if key is None:
            return None

        if args is None: args = []
        if kwargs is None: kwargs = {}

        pure_compose = not (args or kwargs)
        if (key, group) in self._collate_cache and pure_compose:
            return self._collate_cache[(key, group)]

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
                    f'Collation key "{key}" not registered and group "{group}" not implicit'
                )
                return None

            result = method(self, key, *args, **kwargs)
        else:
            method = self.key_registry[key].get(group)
            if method is None:
                logger.debug(
                    f'Collation key "{key}" registered, but group "{group}" is not available'
                )
                return None

            result = method(self, *args, **kwargs)

        if pure_compose:
            self._collate_cache[(key, group)] = result

        return result



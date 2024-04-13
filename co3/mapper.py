'''
Mapper

Used to house useful objects for storage schemas (e.g., SQLAlchemy table definitions).
Provides a general interface for mapping from CO4 class names to storage structures for
auto-collection and composition.

Example:

mapper = Mapper[sa.Table]()

mapper.attach(
    Type,
    attr_comp=TypeTable,
    coll_comp=CollateTable,
    coll_groups={
        'name': NameConversions
    }
)

Development log:
    - Overruled design decision: Mappers were previously designed to map from a specific
      CO3 hierarchy to a specific Schema. The intention was to allow only related types to
      be attached to a single schema, at least under a particular Mapper. The type
      restriction has since been removed, however, as it isn't particularly well-founded.
      During `collect()`, a particular instance collects data from both its attributes and
      its collation actions. It then repeats the same upward for parent types (part of the
      same type hierarchy), and down to components (often not part of the same type
      hierarchy). As such, to fully collect from a type, the Mapper needs to leave
      registration open to various types, not just those part of the same hierarchy.
'''
from typing import Callable
from collections import defaultdict

from co3.co3       import CO3
from co3.collector import Collector
from co3.composer  import Composer
from co3.component import Component
from co3.schema    import Schema


class Mapper[C: Component]:
    '''
    Mapper base class for housing schema components and managing relationships between CO3
    types and storage components (of type C).

    Mappers are responsible for two primary tasks:

    1. Attaching CO3 types to database Components from within a single schema
    2. Facilitating collection of Component-related insertion data from instances of
       attached CO3 types

    Additionally, the Mapper manages its own Collector and Composer instances. The
    Collector receives the inserts from `.collect()` calls, and will subsequently be
    "dropped off" at an appropriate Database's Manager to actually perform the requested
    inserts (hence why we tie Mappers to Schemas one-to-one). 

    Dev note: the Composer needs reconsideration, or at least its positioning directly in
    this class. It may be more appropriate to have at the Schema level, or even just
    dissolved altogether if arbitrary named Components can be attached to schemas.
    '''
    _collector_cls: type[Collector[C]] = Collector[C]
    _composer_cls:  type[Composer[C]]  = Composer[C]

    def __init__(self, schema: Schema[C]):
        '''
        Parameters:
            schema: Schema object holding the set of components eligible as attachment
                    targets for registered CO3 types
        '''
        self.schema = schema

        self.collector = self._collector_cls(schema)
        self.composer  = self._composer_cls()

        self.attribute_comps:  dict[type[CO3], C] = {}
        self.collation_groups: dict[type[CO3], dict[str|None, C]] = defaultdict(dict)

    def _check_component(self, comp: C | str):
        if type(comp) is str:
            comp_key = comp
            comp = self.schema.get_component(comp_key)
            if comp is None:
                raise ValueError(
                    f'Component key {comp_key} not available in attached schema'
                )
        else:
            if comp not in self.schema:
                raise TypeError(
                    f'Component {comp} not registered to Mapper schema {self.schema}'
                )

        return comp

    def attach(
        self,
        type_ref    : type[CO3],
        attr_comp   : C | str,
        coll_comp   : C | str | None      = None,
        coll_groups : dict[str | None, C | str] = None
    ) -> None:
        '''
        Parameters:
            type_ref:    CO3 subtype to map to provided storage components
            attr_comp:   storage component for provided type's canonical attributes
            coll_comp:   storage component for provided type's default/unnamed collation
                         targets
            coll_groups: storage components for named collation groups; dict mapping group
                         names to components
        '''
        # check attribute component in registered schema
        attr_comp = self._check_component(attr_comp)
        self.attribute_comps[type_ref] = attr_comp

        # check default component in registered schema
        if coll_comp is not None:
            coll_comp = self._check_component(coll_comp)
            self.collation_groups[type_ref][None] = coll_comp

        # check if any component in group dict not in registered schema
        if coll_groups is not None:
            for coll_key in coll_groups:
                coll_groups[coll_key] = self._check_component(coll_groups[coll_key])

            self.collation_groups[type_ref].update(coll_groups)

    def attach_many(
        self,
        type_list: list[type[CO3]],
        attr_name_map: Callable[[type[CO3]], str],
        coll_name_map: Callable[[type[CO3], str|None], str] | None = None,
    ):
        '''
        Auto-register a set of types to the Mapper's attached Schema. Associations are
        made from types to both attribute and collation component names, through
        `attr_name_map` and `coll_name_map`, respectively. Collation targets are inferred
        through the registered groups in each type.

        Parameters:
            type_ref:      reference to CO3 type
            attr_name_map: function mapping from types/classes to attribute component names
                           in the attached Mapper Schema
            coll_name_map: function mapping from types/classes & action groups to
                           collation component names in the attached Mapper Schema. `None`
                           is passed as the action group to retrieve the default
                           collection target.
        '''
        for _type in type_list:
            attr_comp = attr_name_map(_type)
            coll_groups = {}
            for action_group in _type.group_registry:
                coll_groups[action_group] = coll_name_map(_type, action_group)

            self.attach(_type, attr_comp, coll_groups=coll_groups)

    def get_attribute_comp(
        self,
        type_ref: type[CO3]
    ) -> C | None:
        return self.attribute_comps.get(type_ref, None)

    def get_collation_comp(
        self,
        type_ref: type[CO3],
        group=str | None
    ) -> C | None:
        return self.collation_groups.get(type_ref, {}).get(group, None)

    def collect(
        self,
        obj: CO3,
        action_keys: list[str]=None,
        action_groups: list[str]=None,
    ) -> list:
        '''
        Stages inserts up the inheritance chain, and down through components.

        Note:
            Even with ORM, a method like this would be needed to trace up parent tables and
            how inserts should be handled for inheritance. ORM would make component
            inserts a little easier perhaps, since they can be attached as attributes to
            constructed row objects and a sa.Relationship will handle the rest. Granted,
            we don't do a whole lot more here: we just call `collect` over those
            components, adding them to the collector session all the same.

        Returns: dict with keys and values relevant for associated SQLite tables
        '''
        if action_keys is None:
            action_keys = list(obj.action_map.keys())

        receipts = []
        for _cls in reversed(obj.__class__.__mro__[:-2]):
            attribute_component = self.get_attribute_comp(_cls)

            # require an attribute component for type consideration
            if attribute_component is None:
                continue

            self.collector.add_insert(
                attribute_component,
                obj.attributes,
                receipts=receipts,
            )

            for action_key in action_keys:
                collation_data = obj.collate(action_key)

                # if method either returned no data or isn't registered, ignore
                if collation_data is None:
                    continue

                _, action_groups = obj.action_registry[action_key]
                for action_group in action_groups:
                    collation_component = self.get_collation_comp(_cls, group=action_group)

                    if collation_component is None:
                        continue

                    # gather connective data for collation components
                    connective_data = obj.collation_attributes(action_key, action_group)

                    self.collector.add_insert(
                        collation_component,
                        {
                            **connective_data,
                            **collation_data, 
                        },
                        receipts=receipts,
                    )

        # handle components
        for comp in [c for c in obj.components if isinstance(c, CO3)]:
            receipts.extend(comp.collect(collector, formats=formats))

        return receipts


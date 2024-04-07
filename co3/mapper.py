'''
Mapper

Used to house useful objects for storage schemas (e.g., SQLAlchemy table definitions).
Provides a general interface for mapping from CO4 class names to storage structures for
auto-collection and composition.

Example:

mapper = Mapper[sa.Table]()

mapper.attach(
    Type,
    attributes=TypeTable,
    collation=CollateTable,
    collation_groups={
        'name': NameConversions
    }
)
'''
from typing import Callable, Self
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
    '''
    _collector_cls: type[Collector[C, Self]] = Collector[C, Self]
    _composer_cls:  type[Composer[C, Self]]  = Composer[C, Self]

    def __init__(self, co3_root: type[CO3], schema: Schema):
        self.co3_root = co3_root
        self.schema   = schema

        self.collector = self._collector_cls()
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
        # check for type compatibility with CO3 root
        if not issubclass(type_ref, self.co3_root):
            raise TypeError(
                f'Type ref {type_ref} not a subclass of Mapper CO3 root {self.co3_root}'
            )

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

    def attach_hierarchy(
        self,
        type_ref: type[CO3],
        obj_name_map: Callable[[type[CO3]], str],
    ):
        pass

    def get_connective_data(
        self,
        type_instance: CO3,
        action_key,
        action_group=None,
    ) -> dict:
        '''
        Return data relevant for connecting collation entries to primary storage
        containers. This is typically some combination of the action key and a unique
        identifier from the target CO3 instance to form a unique key for inserts from
        actions. This is called just prior to collector send-off for collation inserts and
        injected alongside collation data.
        '''
        return {}

    def get_attribute_comp(self, type_ref: CO3) -> C | None:
        return self.attribute_comps.get(type_ref, None)

    def get_collation_comp(self, type_ref: CO3, group=str | None) -> C | None:
        return self.collation_group.get(type_ref, {}).get(group, None)

    def collect(self, obj, action_keys=None) -> dict:
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

                _, action_groups = obj.action_map[action_key]
                for action_group in action_groups:
                    collation_component = self.get_collation_comp(_cls, group=action_group)

                    if collation_component is None:
                        continue

                    # gather connective data for collation components
                    connective_data = self.get_connective_data(_cls, action_key, action_group)

                    collector.add_insert(
                        collation_component,
                        {
                            **connective_data,
                            **collation_data, 
                        },
                        receipts=receipts,
                    )

        # handle components
        for comp in self.components:
            receipts.extend(comp.collect(collector, formats=formats))

        return receipts


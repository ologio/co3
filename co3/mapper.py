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
from collections import defaultdict

from co3.co3 import CO3
from co3.relation import Relation


class Mapper[R: Relation]:
    '''
    Mapper base class for housing schema components and managing relationships between CO3
    types and storage targets (of type R).
    '''
    def __init__(self):
        self.attribute_comps:  dict[CO3, R] = {}
        self.collation_groups: dict[CO3, dict[str|None, R]] = defaultdict(dict)

    def attach(
        self,
        type_ref    : CO3,
        attr_comp   : R,
        coll_comp   : R | None            = None,
        coll_groups : dict[str | None, R] = None
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
        self.attribute_comps[type_ref] = attr_comp

        if coll_comp is not None:
            self.collation_groups[type_ref][None] = attr_comp

        if coll_groups is not None:
            self.collation_groups[type_ref].update(attr_comp)

    def join_attribute_relations(self, r1: R, r2: R) -> R:
        '''
        Specific mechanism for joining attribute-based relations.
        '''
        pass

    def join_collation_relations(self, r1: R, r2: R) -> R:
        '''
        Specific mechanism for joining collation-based relations.
        '''
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

    def get_attribute_comp(self, type_ref: CO3) -> R | None:
        return self.attribute_comps.get(type_ref, None)

    def get_collation_comp(self, type_ref: CO3, group=str | None) -> R | None:
        return self.collation_group.get(type_ref, {}).get(group, None)

    def collect(self, collector, mapper, action_keys=None) -> dict:
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
            action_keys = list(self.action_map.keys())

        receipts = []
        for _cls in reversed(self.__class__.__mro__[:-2]):
            attribute_component = mapper.get_attribute_comp(_cls)

            # require an attribute component for type consideration
            if attribute_component is None:
                continue

            collector.add_insert(
                attribute_component,
                self.attributes,
                receipts=receipts,
            )

            for action_key in action_keys:
                collation_data = self.collate(action_key)

                # if method either returned no data or isn't registered, ignore
                if collation_data is None:
                    continue

                _, action_groups = self.action_map[action_key]
                for action_group in action_groups:
                    collation_component = mapper.get_collation_comp(_cls, group=action_group)

                    if collation_component is None:
                        continue

                    # gather connective data for collation components
                    connective_data = mapper.get_connective_data(self, action_key, action_group)

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

    @classmethod
    def compose(cls, outer=False, conversion=False, full=False):
        '''
        Note:
            Comparing to ORM, this method would likely also still be needed, since it may
            not be explicitly clear how some JOINs should be handled up the inheritance
            chain (for components / sa.Relationships, it's a little easier).

        Parameters:
            outer: whether to use outer joins down the chain
            conversion: whether to return conversion joins or base primitives
            full: whether to return fully connected primitive and conversion table
        '''
        def join_builder(outer=False, conversion=False):
            head_table = None
            last_table = None
            join_table = None

            for _cls in reversed(cls.__mro__[:-2]):
                table_str    = None
                table_prefix = _cls.table_prefix

                if conversion: table_str = f'{table_prefix}_conversions'
                else:          table_str = f'{table_prefix}s'

                if table_str not in tables.table_map:
                    continue

                table = tables.table_map[table_str]

                if join_table is None:
                    head_table = table
                    join_table = table
                else:
                    if conversion:
                        join_condition = last_table.c.name_fmt == table.c.name_fmt
                    else:
                        join_condition = last_table.c.name == table.c.name

                    join_table = join_table.join(table, join_condition, isouter=outer)

                last_table = table

            return join_table, head_table

        if full:
            # note how the join isn't an OUTER join b/w the two
            core, core_h = join_builder(outer=outer, conversion=False)
            conv, conv_h = join_builder(outer=outer, conversion=True)
            return core.join(conv, core_h.c.name == conv_h.c.name)

        join_table, _ = join_builder(outer=outer, conversion=conversion)
        return join_table

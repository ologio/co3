from typing import Self

import sqlalchemy as sa

from co3.mapper import Mapper
from co3.component import ComposableComponent


class ComposableMapper[C: ComposableComponent](Mapper[C]):
    def join_attribute_relations(self, r1: C, r2: C) -> C:
        '''
        Specific mechanism for joining attribute-based relations.
        '''
        pass

    def join_collation_relations(self, r1: C, r2: C) -> C:
        '''
        Specific mechanism for joining collation-based relations.
        '''
        pass

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

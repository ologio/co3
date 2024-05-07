'''
Used to house useful objects for storage schemas (e.g., SQLAlchemy table definitions).
Provides a general interface for mapping from CO3 class names to storage structures for
auto-collection and composition.

Example:

.. code-block:: python

    mapper = Mapper[sa.Table]()
    
    mapper.attach(
        Type,
        attr_comp=TypeTable,
        coll_comp=CollateTable,
        coll_groups={
            'name': NameConversions
        }
    )

.. admonition:: Development log

    - Overruled design decision: Mappers were previously designed to map from a specific
      CO3 hierarchy to a specific Schema. The intention was to allow only related types to
      be attached to a single schema, at least under a particular Mapper. The type
      restriction has since been removed, however, as it isn't particularly well-founded.
      During ``collect()``, a particular instance collects data from both its attributes and
      its collation actions. It then repeats the same upward for parent types (part of the
      same type hierarchy), and down to components (often not part of the same type
      hierarchy). As such, to fully collect from a type, the Mapper needs to leave
      registration open to various types, not just those part of the same hierarchy.
'''
import logging
from inspect import signature
from typing import Callable, Any
from collections import defaultdict

from co3.co3        import CO3
from co3.schema     import Schema
from co3.collector  import Collector
from co3.component  import Component
from co3.components import ComposableComponent


logger = logging.getLogger(__name__)

class Mapper[C: Component]:
    '''
    Mapper base class for housing schema components and managing relationships between CO3
    types and storage components (of type C).

    Mappers are responsible for two primary tasks:

    1. Attaching CO3 types to database Components from within a single schema
    2. Facilitating collection of Component-related insertion data from instances of
       attached CO3 types

    Additionally, the Mapper manages its own Collector and Composer instances. The
    Collector receives the inserts from ``.collect()`` calls, and will subsequently be
    "dropped off" at an appropriate Database's Manager to actually perform the requested
    inserts (hence why we tie Mappers to Schemas one-to-one). 

    .. admonition:: Dev note

        The Composer needs reconsideration, or at least its positioning directly in this
        class. It may be more appropriate to have at the Schema level, or even just
        dissolved altogether if arbitrary named Components can be attached to schemas.

        - Consider pushing this into a Mapper factory; on init, could check if provided
          Schema wraps up composable Components or not
    '''
    _collector_cls: type[Collector[C]] = Collector[C]

    def __init__(self, schema: Schema[C]):
        '''
        Parameters:
            schema: Schema object holding the set of components eligible as attachment
                    targets for registered CO3 types
        '''
        self.schema = schema

        self.collector = self._collector_cls(schema)

        self.attribute_comps:  dict[type[CO3], C] = {}
        self.collation_groups: dict[type[CO3], dict[str|None, C]] = defaultdict(dict)

    def _check_component(self, comp: str | C, strict=True):
        if type(comp) is str:
            comp_key = comp
            comp = self.schema.get_component(comp_key)
            if comp is None:
                err_msg = f'Component key "{comp_key}" not available in attached schema'

                if strict:
                    raise ValueError(err_msg)
                else:
                    logger.info(err_msg)
                    return None
        else:
            if comp not in self.schema:
                err_msg = f'Component "{comp}" not registered to Mapper schema {self.schema}'

                if strict:
                    raise TypeError(err_msg)
                else:
                    logger.info(err_msg)
                    return None

        return comp

    def attach(
        self,
        type_ref    : type[CO3],
        attr_comp   : str | C,
        coll_comp   : str | C | None                   = None,
        coll_groups : dict[str | None, str | C] | None = None,
        strict                                         = True,
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
        attr_comp = self._check_component(attr_comp, strict=strict)
        self.attribute_comps[type_ref] = attr_comp

        # check default component in registered schema
        if coll_comp is not None:
            coll_comp = self._check_component(coll_comp, strict=strict)
            self.collation_groups[type_ref][None] = coll_comp

        # check if any component in group dict not in registered schema
        if coll_groups is not None:
            for coll_key in coll_groups:
                coll_groups[coll_key] = self._check_component(coll_groups[coll_key], strict=strict)

            self.collation_groups[type_ref].update(coll_groups)

    def attach_many(
        self,
        type_list: list[type[CO3]],
        attr_name_map: Callable[[type[CO3]], str | C],
        coll_name_map: Callable[[type[CO3], str], str | C] | None = None,
        strict                                                    = False,
    ) -> None:
        '''
        Auto-register a set of types to the Mapper's attached Schema. Associations are
        made from types to both attribute and collation component names, through
        ``attr_name_map`` and ``coll_name_map``, respectively. Collation targets are inferred
        through the registered groups in each type.

        Parameters:
            type_ref:      reference to CO3 type
            attr_name_map: function mapping from types/classes to attribute component names
                           in the attached Mapper Schema
            coll_name_map: function mapping from types/classes & action groups to
                           collation component names in the attached Mapper Schema. ``None``
                           is passed as the action group to retrieve the default
                           collection target.
        '''
        for _type in type_list:
            attr_comp = attr_name_map(_type)
            coll_groups = {}

            if coll_name_map:
                for group in _type.group_registry:
                    coll_groups[group] = coll_name_map(_type, group)

            self.attach(_type, attr_comp, coll_groups=coll_groups, strict=strict)

    def get_attr_comp(
        self,
        co3_ref: CO3 | type[CO3]
    ) -> C | None:
        type_ref = co3_ref
        if isinstance(co3_ref, CO3):
            type_ref = co3_ref.__class__

        return self.attribute_comps.get(type_ref, None)

    def get_coll_comp(
        self,
        co3_ref: CO3 | type[CO3],
        group=str | None,
    ) -> C | None:
        type_ref = co3_ref
        if isinstance(co3_ref, CO3):
            type_ref = co3_ref.__class__

        return self.collation_groups.get(type_ref, {}).get(group, None)

    def collect(
        self,
        obj           : CO3,
        keys   : list[str] = None,
        groups : list[str] = None,
    ) -> list:
        '''
        Stages inserts up the inheritance chain, and down through components.

        Note:
            Even with ORM, a method like this would be needed to trace up parent tables and
            how inserts should be handled for inheritance. ORM would make component
            inserts a little easier perhaps, since they can be attached as attributes to
            constructed row objects and a sa.Relationship will handle the rest. Granted,
            we don't do a whole lot more here: we just call ``collect`` over those
            components, adding them to the collector session all the same.

        Parameters:
            obj: CO3 instance to collect from
            keys: keys for actions to collect from
            group: group contexts for the keys to collect from. If None, explicit group
                   contexts registered for the keys will be inferred (but implicit groups
                   will not be detected).

        Returns: collector receipts for staged inserts
        '''
        # default is to have no actions
        if keys is None:
            keys = []
            #keys = list(obj.key_registry.keys())

        collation_data = defaultdict(dict)
        for key in keys:
            # keys must be defined
            if key is None:
                continue

            logger.debug(f'Collecting for key "{key}"')

            # if groups not specified, dynamically grab those explicitly attached groups
            # for each key
            group_dict = {}
            if groups is None:
                group_dict = obj.key_registry.get(key, {})
            else:
                for group in groups:
                    group_dict[group] = obj.key_registry.get(key, {}).get(group)

            # method regroup: under key, index by method and run once per
            method_groups = defaultdict(list)
            for group_name, group_method in group_dict.items():
                method_groups[group_method].append(group_name)

            logger.debug(f'Equivalence classes: "{list(method_groups.values())}"')

            # collate for method equivalence classes; only need on representative group to
            # pass to CO3.collate to call the method
            key_collation_data = {}
            for collation_method, collation_groups in method_groups.items():
                key_method_collation_data = obj.collate(key, group=collation_groups[0])

                if key_method_collation_data is None:
                    logger.debug(
                        f'Equivalence class "{collation_groups}" yielded no data, skipping'
                    )
                    continue

                for collation_group in collation_groups:
                    # gather connective data for collation components
                    # -> we do this here as it's obj dependent
                    connective_data = obj.collation_attributes(key, collation_group)

                    if connective_data is None:
                        connective_data = {}

                    key_collation_data[collation_group] = {
                        **connective_data,
                        **key_method_collation_data, 
                    }

            collation_data[key] = key_collation_data

        receipts = []
        attributes = obj.attributes

        for _cls in reversed(obj.__class__.__mro__[:-2]):
            attribute_component = self.get_attr_comp(_cls)

            # require an attribute component for type consideration
            if attribute_component is None:
                continue

            self.collector.add_insert(
                attribute_component,
                attributes,
                receipts=receipts,
            )

            for key, key_collation_data in collation_data.items():
                # if method either returned no data or isn't registered, ignore
                if not key_collation_data:
                    continue

                for group, group_collation_data in key_collation_data.items():
                    collation_component = self.get_coll_comp(_cls, group=group)

                    if collation_component is None:
                        continue

                    self.collector.add_insert(
                        collation_component,
                        group_collation_data,
                        receipts=receipts,
                    )

        # handle components
        for comp in [c for c in obj.components if isinstance(c, CO3)]:
            receipts.extend(self.collect(comp, keys=keys, groups=groups))

        return receipts


class ComposableMapper[C: ComposableComponent](Mapper[C]):
    '''
    .. admonition:: class design

        Heavily debating between multiple possible design approaches here. The main
        purpose of this subtype is make clear the need for additional compositional
        mapping details, namely functions that can produce pairwise join conditions for
        both the attribute tree (vertical traversal) and the collation components
        (horizontal traversal). Here's a few remarks:

        - I want the necessary maps to provided/stored *outside* of ``compose`` calls to
          reduce overhead for downstream callers. It's awkward to have think about the
          exact attr-to-attr associations each time you want a type's associated
          composition, especially when they don't change under the same Mapper (i.e.,
          if you have the Mapper reference, the compositional associations should be
          implicit).
        - The barebones spec here appears to be two pairwise "composer" maps: one for
          attribute comps, and one for collation comps. For now I think this makes sense
          as additional init params, but there may later be reason to wrap this up a bit
          more.
        - Considering the full deprecation for the Composer type, or whether this could be
          the place where it serves some purpose. Aesthetically, there's symmetry with the
          ``collect`` and Collector method-type pairing, but that isn't a good enough reason
          to justify a separate type here. The difference is that Collector instances
          actually store type references, whereas the considered Composer type would
          effectively just be a convenient collection of utility functions. Still possibly
          useful, but not as clearly justifiable.
        - If a separate Composer type were to be justified here, it would serve as a
          "reusable connective tissue" for possibly many Mappers with the same kinds of
          edge-wise relationships. Can think of it like this:

          * Schemas collect up "nodes" (Components). These are explicit storage structures
            in a DB, and can include some explicit attribute connections (foreign keys),
            although those are connections made on the DB side.
          * Mappers provide an exoskeleton for a Schema's nodes. It structures Components into
            attributes and collation types, and additionally ties them to external CO3
            types. The handy analogy here has been that attribute comps connect
            *vertically* (in a tree like fashion; point up for parents and down for
            children), and collation comps point *horizontally* (or perhaps more aptly,
            *outward*; at each node in the attribute tree, you have a "circle" of
            collation comps that can point to it, and are not involved as formal tree
            nodes. Can maybe think of these like "ornaments" or bulbs or orbitals).
          * While the Mappers may provide the "bones," there's no way to communicate
            *across* them. While I might know that one attribute is the "parent" of
            another, I don't know *why* that relationship is there. A Composer, or the
            composer details to be provided to this class, serve as the "nerves" to be
            paired with the bone, actually establishing a line of communication. More
            specifically, the nerves here are attribute-based mappings between pairs of
            Components, i.e., (generalized) join conditions.

        - Note that, by the above logic, we should then want/need a type to manage the
          functions provided to ``attach_many``. These functions help automatically
          characterize the shape of the type skeleton in the same way the proposed
          Composer wrapper would. In fact, the barebones presentation here is really just
          the same two function signatures as are expected by that method. The above
          analogy simply made me ask why the "bones" wouldn't be reusable if the "nerves"
          were going to be. So we should perhaps coordinate a decision on this front; if
          one goes, the other must as well. This may also help me keep it simpler for the
          time being.
        - One other aspect of a dedicated Composer type (and by the above point, a
          hypothetical type to aid in ``attach_many`` specification) could have some sort of
          "auto" feature about it. With a clear enough "discovery system," we could
          encourage certain kinds of Schemas and components are named and structured. Such
          an auto-composer could "scan" all components in a provided Schema and attempt to
          find common attributes across tables that are unlinked (i.e., the reused
          column names implicit across types in the attribute hierarchy; e.g., File.name
          -> Note.name), as well as explicit connections which may suggest collation
          attachment (e.g., ``note_conversions.name --FK-> Note.name``). This, of course,
          could always be overridden with manual specification, but being aware of some
          automatic discovery structures could help constrain schema definitions to be
          more in-line with the CO3 operational model. That all being said, this is a
          large amount of complexity and should likely be avoided until necessary.

    .. admonition:: Instance variables

        - ``type_compose_cache``: index for previously computed compositions. This index
                                  is reset if either ``attach`` or ``attach_many`` is
                                  called to allow possible new type propagation.
    '''
    def __init__(
        self,
        schema           : Schema[C],
        attr_compose_map : Callable[[str | C, str | C], Any] | None = None,
        coll_compose_map : Callable[[str | C, str | C], Any] | None = None,
    ):
        super().__init__(schema)

        self.attr_compose_map = attr_compose_map
        self.coll_compose_map = coll_compose_map

        self.type_compose_cache = {}

    def attach(self, *args, **kwargs):
        self.type_compose_cache = {}

        super().attach(*args, **kwargs)
        
    def attach_many(self, *args, **kwargs):
        self.type_compose_cache = {}

        super().attach_many(*args, **kwargs)

    def compose(
        self,
        co3_ref:       CO3 | type[CO3],
        groups: list[str] | None = None,
        *compose_args,
        **compose_kwargs,
    ):
        '''
        Compose tables up the type hierarchy, and across through action groups to
        collation components.

        Note:
            Comparing to ORM, this method would likely also still be needed, since it may
            not be explicitly clear how some JOINs should be handled up the inheritance
            chain (for components / sa.Relationships, it's a little easier).


        .. admonition:: On compose order

        Parameters:
            obj: either a CO3 instance or a type reference
        '''
        type_ref = co3_ref
        if isinstance(co3_ref, CO3):
            type_ref = co3_ref.__class__

        if type_ref in self.type_compose_cache:
            return self.type_compose_cache[type_ref]

        comp_agg = None
        last_attr_comp = None
        last_coll_comps = None
        for _cls in reversed(type_ref.__mro__[:-2]):
            attr_comp = self.get_attr_comp(_cls)

            # require an attribute component for type consideration
            if attr_comp is None:
                continue

            if comp_agg is None:
                comp_agg = attr_comp
            else:
                # note the reduced attr_comp (produced this iteration) and the
                # last_attr_comp (last iteration) refs passed to compose map, rather than
                # their aggregated counterparts. This is because compose conditions often
                # need to be specified between *atomic* components within compositional
                # components, as compositions don't also expose the necessary attributes
                # (or if they do, they aren't necessarily unique; e.g., JOIN two
                # SQLAlchemy tables does not allow direct column access).
                compose_condition = self.attr_compose_map(last_attr_comp, attr_comp)
                comp_agg = comp_agg.compose(
                    attr_comp,
                    compose_condition,
                    *compose_args,
                    **compose_kwargs,
                )

            # compose horizontally with components from provided action groups
            coll_list = []
            if groups is not None:
                for group in groups:
                    coll_comp = self.get_coll_comp(_cls, group=group)

                    if coll_comp is None:
                        continue

                    # valid collation comps added to coll_list, to be passed to the
                    # coll_map in the next iteration
                    coll_list.append(coll_comp)

                    # note how the join condition is specified using the non-composite
                    # `attr_comp` and new `coll_comp`; the composite doesn't typically
                    # have the same attribute access and needs a ref to a specific comp
                    if len(signature(self.coll_compose_map).parameters) > 2:
                        compose_condition = self.coll_compose_map(
                            attr_comp,
                            coll_comp,
                            last_coll_comps
                        )
                    else:
                        compose_condition = self.coll_compose_map(attr_comp, coll_comp)

                    comp_agg = comp_agg.compose(
                        coll_comp,
                        compose_condition,
                        *compose_args,
                        **compose_kwargs,
                    )

            last_attr_comp = attr_comp
            last_coll_comps = coll_list

        self.type_compose_cache[type_ref] = comp_agg

        return comp_agg

'''
Schema

Collection of related storage components, often representing the data structure of an
entire database. Some databases support multiple schemas, however. In general, a Schema
can wrap up an associated subset of components within a single database, so long as
``Manager.recreate()`` supports creating components in separate calls (even if the
associated database doesn't explicitly support multiple schemas).

Schema objects are used to:

- Semantically group related storage components
- Tell databases what components to create/remove together
- Provide target contexts for connected CO3 type systems within Mappers

with particular emphasis on the latter. Mappers associate exactly one CO3 type hierarchy
with exactly one Schema. This is an intentional point of simplification in the CO3
operational model.
'''

from co3.component import Component


class Schema[C: Component]:
    def __init__(self):
        self._component_set = set()
        self._component_map = {}

    def __contains__(self, component: C):
        return component in self._component_set

    def add_component(self, component: C):
        self._component_set.add(component)
        self._component_map[component.name] = component

    def get_component(self, name: str):
        return self._component_map.get(name)

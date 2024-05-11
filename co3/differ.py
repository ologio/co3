from typing import Any
from collections import defaultdict
from abc import ABCMeta, abstractmethod

from co3.util.types import Equatable
from co3.resource import SelectableResource


class Differ[E: Equatable](metaclass=ABCMeta):
    '''
    Compute diff sets (asymmetric exclusives and intersection) among ``Equatable``
    transformations of results from ``SelectableResources``.
    '''
    def __init__(
        self,
        l_resource: SelectableResource,
        r_resource: SelectableResource,
    ):
        self.l_resource = l_resource
        self.r_resource = r_resource

    @abstractmethod
    def l_transform(self, item) -> E:
        '''
        Transform items from the left resource to the joint comparison space, i.e., an
        instance of type ``Equatable``
        '''
        raise NotImplementedError

    @abstractmethod
    def r_transform(self, item) -> E:
        raise NotImplementedError

    def diff(
        self,
        l_select_kwargs: dict,
        r_select_kwargs: dict,
    ) -> tuple[dict[E, Any], dict[E, Any], dict[E, Any]]:
        l_items = self.l_resource.select(**l_select_kwargs)
        r_items = self.r_resource.select(**r_select_kwargs)

        l_map: dict[E, list[Any]] = defaultdict(list)
        r_map: dict[E, list[Any]] = defaultdict(list)

        for item in l_items:
            l_map[self.l_transform(item)].append(item)
            
        for item in r_items:
            r_map[self.r_transform(item)].append(item)

        l_set: set[E] = set(l_map)
        r_set: set[E] = set(r_map)

        l_excl = { l:l_map[l] for l in l_set - r_set }
        r_excl = { r:r_map[r] for r in r_set - l_set }
        lr_int = { i:(l_map[i], r_map[i]) for i in l_set & r_set }

        return l_excl, r_excl, lr_int #, (l_map, r_map)

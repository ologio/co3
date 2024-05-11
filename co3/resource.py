from typing import Protocol


class Resource:
    pass

class SelectableResource(Protocol):
    def select(self, component, *args, **kwargs):
        raise NotImplementedError

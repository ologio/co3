from typing import Protocol


class Resource:
    def content(self) -> BinaryIO:
        pass

class SelectableResource(Protocol):
    def select(self, component, *args, **kwargs):
        raise NotImplementedError

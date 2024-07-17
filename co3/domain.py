from contextlib import contextmanager

from co3 import Resource


class Domain[R: Resource]:
    '''
    General Domain class


    '''
    def __init__(self, content):
        pass
    
    def get_resource(self, url: URL) -> Resource:
        pass

    @contextmanager
    def connect(self, timeout=None):
        raise NotImplementedError

class SelectableDomain(Domain):
    def select(self, component, *args, **kwargs):
        raise NotImplementedError

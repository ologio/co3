'''
Component

General wrapper for storage components to be used in various database contexts. Relations
can be thought of generally as named data containers/entities serving as a fundamental
abstractions within particular storage protocols.
'''

class Component[T]:
    def __init__(self, name, obj: T, schema: 'Schema'):
        self.name = name
        self.obj  = obj

        self.schema = schema
        schema.add_component(self)

    def __str__(self):
        return f'<Component ({self.__class__.__name__})> {self.name}'

    def __repr__(self):
        return f'<Component ({self.__class__.__name__})> {self.name}'

    def get_attributes(self):
        raise NotImplementedError


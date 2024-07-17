import logging
from contextlib import contextmanager


logger = logging.getLogger(__name__)

class Medium[R: Resource]:
    '''
    Medium base class. 

    A Resource space
    '''
    def __init__(self, scope):
        pass
        
    @contextmanager
    def connect(self, timeout=None):
        '''
        Open a connection to the database specified by the resource. Exactly what the
        returned connection looks like remains relatively unconstrained given the wide
        variety of possible database interactions. This function should be invoked in
        with-statement contexts, constituting an "interaction session" with the database
        (i.e., allowing several actions to be performed using the same connection).
        '''
        raise NotImplementedError

    def execute(self, query: Query[QL]):
        pass


class BrowsableMedium[R: Resource](Medium[R]):
    def browse(self, uri: URI[R]):
        '''
        Analog for Read (CRUD), SELECT (SQL), GET (REST)
        '''
        pass


class ABCDMedium[R: Resource](BrowsableMedium[R]):
    def append(self, uri: URI[R], resource: R):
        '''
        Analog for Create (CRUD), INSERT (SQL), POST/PUT (REST)
        '''
        pass

    def change(self, uri: URI[R], resource: R):
        '''
        Analog for Update (CRUD), UPDATE (SQL), PUT/PATCH (REST)

        Can a URI be another object? Component for ex; could inherit from URI I guess
        '''
        pass

    def delete(self, uri: URI[R]):
        '''
        Analog for Delete (CRUD), DELETE (SQL), DELETE (REST)
        '''
        pass

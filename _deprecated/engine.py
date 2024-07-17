import logging
from contextlib import contextmanager


logger = logging.getLogger(__name__)

class Engine:
    '''
    Engine base class. Used primarily as a Database connection manager, with general
    methods that can be defined for several kinds of value stores.

    Note that this is where the connection hierarchy is supposed to stop. While some
    derivative Engines, like SQLEngine, mostly just wrap another engine-like object, this
    is not the rule. That is, inheriting Engine subtypes shouldn't necessarily expect to
    rely on another object per se, and if such an object is required, _this_ is the class
    meant to be the skeleton that supports its creation (and not merely a wrapper for some
    other type, although it may appear that way when such a type is in fact readily
    available).

    .. admonition:: why is this object necessary?

        More specifically, why not just have all the functionality here packed into the
        Database by default? The answer is that, realistically, it could be. The type
        separation between the Engine and Database is perhaps the least substantiated in
        CO3. That being said, it still serves a purpose: to make composition of subtypes
        easier. The Engine is a very lightweight abstraction, but some Engine subtypes
        (e.g., FileEngines) may be used across several sibling Database types. In this
        case, we'd have to repeat the Engine-related functionality for such sibling types.
        Depending instead on a singular, outside object keeps things DRY. If Databases and
        Engines were uniquely attached type-wise 1-to-1 (i.e., unique Engine type per
        unique Database type), a separate object here would indeed be a waste, as is the
        case for any compositional typing scheme.

    .. admonition:: dev note

        This class is now non-generic. It was originally conceived as a generic, depending
        on a "resource spec type" to be help define expected types on initialization.
        This simply proved too messy, required generic type propagation to the Database
        definition, and muddied the otherwise simple args and kwargs forwarding for
        internal manager creation.
    '''
    def __init__(self, *manager_args, **manager_kwargs):
        self._manager        = None
        self._manager_args   = manager_args
        self._manager_kwargs = manager_kwargs

    @property
    def manager(self):
        '''
        Return Engine's singleton manager, initializing when the first call is made.
        '''
        if self._manager is None:
            self._manager = self._create_manager()

        return self._manager

    def _create_manager(self):
        '''
        Create the session manager needed for connection contexts. This method is called
        once by the ``.manager`` property function when it is first accessed. This method is
        separated to isolate the creation logic in inheriting types.

        Note that this method takes no explicit arguments. This is primarily because the
        standard means of invocation (the manager property) is meant to remain generally
        useful here in the base class, and can't be aware of any specific properties that
        might be extracted in subtype initialization. As a result, we don't even try to
        pass args, although it would just look like a forwarding of the readily manager
        args and kwargs anyhow. As such, this method should make direct use of these
        instance variables as needed.
        '''
        raise NotImplementedError
        
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

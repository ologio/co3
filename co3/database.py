'''
Database

Central object for defining storage protocol-specific interfaces. The database wraps up
central items for interacting with database resources, namely the Accessor and Manager
objects.

The Database type hierarchy attempts to be exceedingly general; SQL-derivatives should
subclass from the RelationalDatabase subtype, for example, which itself becomes a new
generic via a type dependence on Relation.

While relying no many constituent pieces, Databases intend to provide all needed objects
under one roof. This includes the Engine (opens up connections to the database), Accessors
(running select-like queries on DB data), Managers (updating DB state with sync
insert-like actions), and Indexers (systematically caching Accessor queries). Generalized
behavior is supported by explicitly leveraging the individual components. For example,

.. code-block:: python

    with db.engine.connect() as connection:
        db.access.select(
            connection,
            <query>
        )
        db.manager.insert(
            connection,
            component,
            data
        )

The Database also supports a few directly callable methods for simplified interaction.
These methods manage a connection context internally, passing them through the way they
might otherwise be handled explicitly, as seen above.

.. code-block:: python

    db.select(<query>)
    
    db.insert(<query>, data)


.. admonition:: on explicit connection contexts

    Older models supported Accessors/Managers that housed their own Engine instances, and
    when performing actions like ``insert``, the Engine would be passed all the way through
    until a Connection could be spawned, and in that context the single action would be
    made. This model forfeits a lot of connection control, preventing multiple actions
    under a single connection.

    The newer model now avoids directly allowing Managers/Accessors access to their own
    engines, and instead they expose methods that explicitly require Connection objects.
    This means a user can invoke these methods in their own Connection contexts (seen
    above) and group up operations as they please, reducing overhead. The Database then
    wraps up a few single-operation contexts where outer connection control is not needed.
'''

import logging

from co3.accessor import Accessor
from co3.manager  import Manager
from co3.indexer  import Indexer
from co3.engine   import Engine
from co3.schema   import Schema

logger = logging.getLogger(__name__)


class Database[C: Component]:
    '''
    Generic Database definition

    Generic to both a Component (C), and an Engine resource type (R). The Engine's
    generic openness must be propagated here, as it's intended to be fully abstracted away
    under the Database roof. Note that we cannot explicitly use an Engine type in its
    place, as it obscures its internal resource type dependence when we need it for
    hinting here in ``__init__``.

    .. admonition:: Development TODO list

        Decide on official ruling for assigning Schema objects, and verifying any
        attempted Component-based actions (e.g., inserts, selects) to belong to or be a
        composition of Components within an attached Schema. Reasons for: helps complete
        the sense of a "Database" here programmatically, incorporating a more structurally
        accurate representation of allowed operations, and prevent possible attribute and
        type collisions. Reasons against: generally not a huge concern to align Schemas as
        transactions will rollback, broadly increases a bit of bulk, and users often
        expected know which components belong to a particular DB. Leaning more to **for**,
        and would only apply to the directly supported method passthroughs (and thus would
        have no impact on independent methods like ``Accessor.raw_select``). Additionally,
        even if component clashes don't pose serious risk, it can be helpful to
        systematically address the cases where a misalignment is occurring (by having
        helpful ``verify`` methods that can be ran before any actions).
    '''
    _accessor_cls: type[Accessor[C]] = Accessor[C]
    _manager_cls:  type[Manager[C]]  = Manager[C]
    _engine_cls:   type[Engine]      = Engine

    def __init__(self, *engine_args, **engine_kwargs):
        '''
        Parameters:
            engine_args: positional arguments to pass on to the Engine object during
                         instantiation 
            engine_kwargs: keyword arguments to pass on to the Engine object during
                           instantiation 

        Variables:
            _local_cache: a database-local property store for ad-hoc CacheBlock-esque
                          methods, that are nevertheless _not_ query/group-by responses to
                          pass on to the Indexer. Dependent properties should write to the
                          this cache and check for existence of stored results; the cache
                          state must be managed globally.
        ''' 
        self.engine = self._engine_cls(*engine_args, **engine_kwargs)

        self.accessor = self._accessor_cls()
        self.manager  = self._manager_cls()
        self.indexer  = Indexer(self.accessor)

        self._local_cache = {}
        self._reset_cache = False

    def select(self, component: C, *args, **kwargs):
        '''
        .. admonition:: Dev note

            args and kwargs have to be general/unspecified here due to the possible
            passthrough method adopting arbitrary parameters in subtypes. I could simply
            overload this method in the relevant inheriting DBs (i.e., by matching the
            expected Accessor's .select signature).
        '''
        with self.engine.connect() as connection:
            return self.accessor.select(
                connection,
                component,
                *args,
                **kwargs
            )

    def insert(self, component: C, *args, **kwargs):
        with self.engine.connect() as connection:
            return self.manager.insert(
                connection,
                component,
                *args,
                **kwargs
            )

    def recreate(self, schema: Schema[C]):
        self.manager.recreate(schema, self.engine)

    @property
    def index(self):
        if self.reset_cache:
            self._index.cache_clear()
            self.reset_cache = False
        return self._index

    @property
    def manage(self):
        '''
        Accessing ``.manage`` queues a cache clear on the external index, as well wipes the
        local index.
        '''
        self.reset_cache = True
        self._local_cache = {}
        return self._manage

    def populate_indexes(self): pass


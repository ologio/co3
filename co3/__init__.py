'''
Database submodule

- `db`: contains SQLAlchemy-based schema definitions
- `accessors`: convenience methods for accessing database entries
- `populate`: convenience methods for populating database tables

The `accessors` and `populate` submodules are each split into `schema` and `fts` method
groups. The former concerns methods relating to the actual database schema, the latter to
their SQLite FTS counterparts.

Note: Subpackages organization
    Subpackages are broken up by inheritance. Within a given submodule, you have a
    `_base.py` file defining the base class associated with that submodule's title, along
    with concrete subclasses of that base in their own files. Deeper inheritance would
    recursively extend this structure. The `__init__.py` for a given submodule then
    exposes the concrete instances, leaving the base hidden. For example,

    accessors/
        _base.py
        core.py
        fts.py

    `core` and `fts` house the `CoreAccessor` and `FTSAccessor` classes, respectively,
    and are the direct subclasses of the `Accessor` parent found in the `_base`. This base
    class _could_ be placed outside of the submodule in the parent directory (imported
    with something like `from db import accessor` instead of `from db.accessor import
    _base`). This is entirely valid, but I tend to prefer when the base class is among its
    direct children, as

    - In this case at least, the base doesn't need to be exposed
    - The base class is being stowed away under an appropriately named submodule; having a
      separate `accessor.py` and `accessors/` file/directory can feel a little cluttered.
    - It makes imports across the accessors feel standardized:

      ```py
      from localsys.db.accessors._base import Accessor

      from localsys.db.accessors.core import CoreAccessor
      ```

      Both have the same level of nesting to reach the class.

    Frankly, both means of organization are perfectly fine, and as far as I can tell,
    semantically sound in their own right. This particular scheme is just a preference in
    the moment, and so long as I keep things consistent, choosing one over the other
    shouldn't matter.

    Additionally, note how `__init__.py`s are typically set up when providing wider access
    to internal modules. The `init` typically pulls out classes from sibling modules
    (i.e., files), but will import subpackages are the topmost level. For example, for the
    structure

    ```
    db/
        __init__.py
        accessors/
            __init__.py
            _base.py
            core.py
            fts.py
    ```

    we have

    ```db/__init__.py
    from localsys.db import accessors
    ```

    which just imports the subpackage `accessors`. However, within subpackage:

    ```db/accessors/__init__.py
    from localsys.db.accessors.core import CoreAccessor
    ```

    we don't just import the submodule `core`; we did into the file to grab the relevant
    class and pull it into the outer namespace. Overarching point: `__init__.py` files
    typically reach into the sibling files (submodules) and pull out classes. Given that
    this behavior is recursive, `__init__.py` then respect subpackages (nested
    directories), importing them at the top-level and expecting an internal `__init__.py`
    will have managed access appropriately.

Note: Organization for inheritance over composition
    At a glance, the organization of subpackages here feels like it clashes with those
    seen in `localsys.primitives`. `note_components`, for instance, houses the components
    for the outer `note` module. Contrast this with how the `core` submodule looks: it's
    composing `*/core.py` files across subpackages `accessors` and `managers`, rather than
    a single subpackage like `note`. This seems inconsistent, but the subpackages here are
    actually still organized in the same way: by inheritance. It just happens that the
    all of the note components inherit from the same base class, and are thus confined to
    a single subpackage. This aside, the subpackages themselves are still created around
    inheritance, wrapping up a base and direct subclasses.
'''

from co3.accessor  import Accessor
from co3.co3       import CO3, collate
from co3.collector import Collector
#from co3.composer  import Composer
from co3.database  import Database
from co3.indexer   import Indexer
from co3.manager   import Manager
from co3.mapper    import Mapper, ComposableMapper
from co3.component import Component
from co3.schema    import Schema
from co3.engine    import Engine

from co3 import accessors
from co3 import databases
from co3 import managers
from co3 import components
from co3 import schemas
from co3 import engines
from co3 import util

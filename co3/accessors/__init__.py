'''
Note that subclasses in this subpackage are split differently to other subpackages in the
DB. Instead of being split by table group, corresponding to a Composer (which defines that
table group), Accessors are split by a separate dimension: table "type". This is why we
have a "TableAccessor" and an "FTSAccessor": the former exposes access operations
available to generic tables, the latter to FTS tables (instead of being designed
specifically around "core" and "fts" groups, for instance).

Seeing as FTS tables are "generic" tables, it seems inconsistent not to have FTSAccessor
inherit from TableAccessor. While this would work fine, the model we're working with
doesn't really need it; you can instead think of the FTSAccessor as defining _only_
FTS-specific operations. Given that you have a Composer for your desired table group, you
can then wrap it with your desired set of "access actions," available in separate Accessor
subclasses.

For instance, you could wrap an FTSComposer in either a TableAccessor or FTSAccessor. The
former will treat the tables in the composer like regular tables, exposing methods like
``.select`` and ``.select_one``, whereas the latter defines FTS-specific actions like
``.search``.
'''

from co3.accessors.sql import SQLAccessor
from co3.accessors.fts import FTSAccessor
from co3.accessors.vss import VSSAccessor

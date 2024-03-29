from co3.database import Database

from co3.accessors.fts import FTSAccessor
from co3.managers.fts  import FTSManager

class FTSDatabase(Database):
    accessor = FTSAccessor
    manager  = FTSManager

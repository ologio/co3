from co3.database import Database

from co3.accessors.vss import VSSAccessor
from co3.managers.vss  import VSSManager


class VSSDatabase(Database):
    accessor = VSSAccessor
    manager  = VSSManager

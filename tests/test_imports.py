def test_import():
    from co3 import Accessor
    from co3 import CO3
    from co3 import Collector
    from co3 import Database
    from co3 import Indexer
    from co3 import Manager
    from co3 import Mapper
    from co3 import Component


    from co3.accessors import SQLAccessor
    from co3.accessors import FTSAccessor
    from co3.accessors import VSSAccessor

    from co3.databases import SQLDatabase
    from co3.databases import SQLiteDatabase

    from co3.managers import SQLManager

    from co3.components import ComposableComponent
    from co3.components import Relation
    from co3.components import SQLTable

    assert True

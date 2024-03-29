# Overview
`co3` is a package for file conversion and associated database operations. The `CO3` base class
provides a standard interface for performing conversions, preparing inserts, and
interacting with database schemas that mirror the class hierarchy.

Simplified description of the operational model:

**Goal**: interact with a storage medium (database, pickled structure, VSS framework) with
a known schema.

- **Accessor** to provide access to stored items
- **Composer** to compose common access points (e.g., JOINed tables)
- **Indexer** to index/cache access queries
- **Manager** to manage storage state (e.g., supported inserts, database syncs)
- **Collector** to collect data for updating storage state
- **Database** to collect data for updating storage state
- **Mapper** to collect data for updating storage state
- **Relation** to collect data for updating storage state

**CO3** is an abstract base class that makes it easy to integrate this model with object
hierarchies that mirror a storage schema.

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
- **Component** to collect data for updating storage state

**CO3** is an abstract base class that makes it easy to integrate this model with object
hierarchies that mirror a storage schema.

# Detailed structural breakdown
There are a few pillars of the CO3 model that meaningfully group up functionality:

- Database: generic to a Component type, provides basic connection to a database at a
  specific address/location. The explicit Component type makes it easy to hook into
  appropriately typed functional objects:
  * Manager: generic to a Component and Database type, provides a supported set of
    state-modifying operations to a constituent database
  * Accessor: generic to a Component and Database type, provides a supported set of
    state inspection operations on a constituent database
  * Indexer: 
- Mapper: generic to a Component, serves as the fundamental connective component between
  types in the data representation hierarchy (CO3 subclasses) and database Components.

# Overview
`co3` is a lightweight Python ORM for hierarchical storage management. It implements a
general type system for defining database components like relations, schemas, engines,
etc. Objects inheriting from the `CO3` base class can then define data transformations
that connect to database components, and can be automatically collected for coordinated
database insertion.

`co3` attempts to provide a general interface for interacting with a storage media (e.g.,
database, pickled objects, VSS framework, in-memory key-value stores, etc). The following
top-level classes capture the bulk of the operational model:

- **Database**: reference to a storage medium, with an `Accessor` for accessing data,
  `Manager` for managing database state, and an `Engine` for managing connections and
  external operations.
- **Accessor**: provides access to stored items in a `Database`, typically via a supported
  `select` operation over known `Component` types
- **Manager**: manages database storage state (e.g., supported inserts or database sync
  operations)
- **Mapper**: associates `CO3` types with `Schema` components, and provides automatic
  collection and composition operations for supported items
- **Collector**: collects data from defined `CO3` type transformations and prepares for
  `Database` insert operations
- **Component**: atomic storage groups for databases (i.e., generalized notion of a
  "relation" in relational algebra).
- **Indexer**: automatic caching of supported access queries to a `Database`
- **Schema**: general schema analog for grouping related `Component` sets
- **Differ**: facilitates set operations on results from selectable resources (e.g.,
  automatic comparison between file data on disk and file rows in a SQL database)
- **Syncer**: generalized syncing procedure for items between data resources (e.g.,
  syncing new, modified, and deleted files from disk to a SQL database that stores file
  metadata).

The **CO3** an abstract base class then makes it easy to integrate this model with regular
Python object hierarchies that can be mapped to a storage schema.


import time
import logging
import threading
from collections import defaultdict
from collections.abc import Iterable

import sqlalchemy as sa


logger = logging.getLogger(__name__)

class Indexer:
    '''
    Indexer class

    Provides restricted access to an underlying Accessor to enable more efficient, superficial
    caching.

    Cache clearing is to be handled by a wrapper class, like the Database.

    Caching occurs at the class level, with indexes prefixed by table's origin Composer.
    This means that cached selects/group-bys will be available regardless of the provided
    Accessors so long as the same Composer is used under the hood. 
    '''
    _cls_select_cache  = {}
    _cls_groupby_cache = defaultdict(dict)
    
    def __init__(self, accessor, cache_select=True, cache_groupby=True):
        self.accessor = accessor

        # set instance caches; if remains None, methods can't index
        self._select_cache = None
        self._groupby_cache = None

        if cache_groupby and not cache_select:
            raise ValueError('cannot cache groupbys without select caching enabled')

        if cache_select:
            self._select_cache = self._cls_select_cache

        if cache_groupby:
            self._groupby_cache = self._cls_groupby_cache

        self._access_lock = threading.Lock()

    def cache_clear(self, group_by_only=False): 
        self._groupby_cache.clear()
        if not group_by_only:
            self._select_cache.clear()

    def cache_block(
        self,
        table,
        **kwargs,
    ):
        '''
        Provide a user-friendly, dynamically re-indexable
        '''
        return CacheBlock(
            indexer = self,
            table   = table,
            **kwargs,
        )

    def cached_query(
        self,
        table, 

        cols        = None,
        where       = None,
        distinct_on = None,
        order_by    = None,
        limit       = 0,

        group_by    = None,
        agg_on      = None,
        index_on    = None,
    ):
        '''
        Like ``group_by``, but makes a full query to the Accessors table ``table_name`` and
        caches the results. The processing performed by the GROUP BY is also cached.

        Update: ``cached_select`` and ``cached_group_by`` now unified by a single
        ``cached_query`` method. This allows better defined GROUP BY caches, that are
        reactive to the full set of parameters returning the result set (and not just the
        table, requiring a full query).

        Note: on cache keys
            Cache keys are now fully stringified, as many objects are now allowed to be
            native SQLAlchemy objects. Indexing these objects works, but doing so will
            condition the cache on their memory addresses, which isn't what we want.
            SQLAlchemy converts most join/column/table-like objects to reasonable strings,
            which will look the same regardless of instance.

            Context: this became a clear issue when passing in more
            ``order_by=<col>.desc()``. The ``desc()`` causes the index to store the column in
            an instance-specific way, rather than an easily re-usable, canonical column
            reference. Each time the CoreDatabase.files() was being called, for instance,
            that @property would be re-evaluated, causing ``desc()`` to be re-initialized,
            and thus look different to the cache. Stringifying everything prevents this
            (although this could well be an indication that only a single ``cache_block``
            should ever be returned be database properties).

        Note: on access locks
            A double-checked locking scheme is employed before both of the stages (select
            and manual group by), using the same lock. This resolves the common scenario
            where many threads need to look up a query in the cache, experience a cache
            miss, and try to do the work. This non-linearly explodes the total time to
            wait in my experience, so doing this only when needed saves tons of time,
            especially in high-congestion moments.
        '''
        start     = time.time()
        cache_key = tuple(map(str, (table, cols, where, distinct_on, order_by, limit)))

        # apparently this is the double-check locking scheme (didn't realize when implementing)
        if self._select_cache is None or cache_key not in self._select_cache:
            # cache re-compute possible, acquire lock to continue. A later thread may
            # acquire this after work has been done by an earlier thread, so re-eval the
            # condition below before actually performing a DB read. If access results in a
            # cache hit, locking isn't needed.
            with self._access_lock:
                if self._select_cache is None or cache_key not in self._select_cache:
                    results = self.accessor.select(
                        table,
                        cols=cols,
                        where=where,
                        distinct_on=distinct_on,
                        order_by=order_by,
                        limit=limit,
                        mappings=True
                    )

                    # cache results if select_cache is defined
                    if self._select_cache is not None: 
                        self._select_cache[cache_key] = results

                    logger.debug(
                        f'Indexer "select" cache miss for table "{table}": access in {time.time()-start:.4f}s'
                    )
                else:
                    results = self._select_cache[cache_key]
                    logger.debug(
                        f'Indexer "select" cache hit for table "{table}": access in {time.time()-start:.4f}s'
                    )
        else:
            results = self._select_cache[cache_key]
            logger.debug(
                f'Indexer "select" cache hit for table "{table}": access in {time.time()-start:.4f}s'
            )

        start     = time.time()
        cache_key = (*cache_key, group_by, agg_on, index_on)

        if group_by is not None:
            if self._groupby_cache is None or cache_key not in self._groupby_cache:
                with self._access_lock:
                    if self._groupby_cache is None or cache_key not in self._groupby_cache:
                        results = self.group_by(
                            results,
                            group_by     = group_by,
                            agg_on       = agg_on,
                            index_on     = index_on,
                            return_index = True,
                        )

                        if self._groupby_cache is not None:
                            self._groupby_cache[cache_key] = results

                        logger.debug(
                            f'Indexer "group_by" cache miss for table "{table}": access in {time.time()-start:.4f}s'
                        )
                    else:
                        results = self._groupby_cache[cache_key]
                        logger.debug(
                            f'Indexer "group_by" cache hit for table "{table}": access in {time.time()-start:.4f}s'
                        )
            else:
                results = self._groupby_cache[cache_key]
                logger.debug(
                    f'Indexer "group_by" cache hit for table "{table}": access in {time.time()-start:.4f}s'
                )

        return results
        
    @classmethod
    def group_by(
        cls,
        rows,
        group_by,
        agg_on=None,
        index_on=None,
        return_index=False,
    ):
        '''
        Post-query "group by"-like aggregation. Creates an index over a set of columns
        (``group_by_cols``), and aggregates values from ``agg_cols`` under the groups.

        Rows can be dicts or mappings, and columns can be strings or SQLAlchemy columns.
        To ensure the right columns are being used for the operation, it's best to pass in
        mappings and use SQA columns if you aren't sure exactly how the keys look in your
        results (dicts can have ambiguous keys across tables with the same columns and/or
        different labeling schemes altogether).

        TODO: add a flag that handles None's as distinct. That is, for the group_by
        column(s) of interest, if rows in the provided query set have NULL values for
        these columns, treat all such rows as their "own group" and return them alongside
        the grouped/aggregated ones. This is behavior desired by something like
        FTSManager.recreate(), which wants to bundle up conversions for blocks
        (effectively grouping by blocks.name and link.id, aggregating on
        block_conversions.format, then flattening). You could either do this, or as the
        caller just make sure to first filter the result set before grouping (e.g.,
        splitting the NULL-valued rows from those that are well-defined), and then
        stitching the two sets back together afterward.

        Multi-dim update:
        
        - group_by: can be a tuple of tuples of columns. Each inner tuple is a nested
          "group by index" in the group by index
        - 
        '''
        if not rows:
            return {} if return_index else []

        rows_are_mappings = not isinstance(rows[0], dict)

        if not rows_are_mappings:
            if isinstance(group_by, sa.Column):
                group_by = group_by.name
            else:
                group_by = str(group_by)

        #if group_by is None:                     group_by = []
        #elif not isinstance(group_by, Iterable): group_by = [group_by]

        if agg_on is None:                     agg_on = []
        elif not isinstance(agg_on, Iterable): agg_on = [agg_on]

        if index_on is None:                     index_on = []
        elif not isinstance(index_on, Iterable): index_on = [index_on]

        agg_on_names = []
        for agg in agg_on:
            # if a SQA column, can either use `.name` or `str(c)`. The latter includes the
            # table name, the former doesn't; ambiguity can be introduced here.
            if isinstance(agg, sa.Column):
                agg_on_names.append(agg.name)
            else:
                agg_on_names.append(str(agg))

        index_on_names = []
        for index in index_on:
            # if a SQA column, can either use `.name` or `str(c)`. The latter includes the
            # table name, the former doesn't; ambiguity can be introduced here.
            if isinstance(index, sa.Column):
                index_on_names.append(index.name)
            else:
                index_on_names.append(str(index))

        # when rows are dicts, use columns' string names
        if not rows_are_mappings:
            agg_on = agg_on_names
            index_on = index_on_names

        #print(f'rows_are_mappings: {rows_are_mappings}')
        #print(f'group_by: {group_by}')
        #print(f'agg_on: {agg_on}')
        #print(f'agg_on_names: {agg_on_names}')
        #print(f'index_on: {index_on}')
        #print(f'index_on_names: {index_on_names}')

        # "group by" block ID and wrangle the links into a list
        group_by_idx = {}
        for row in rows:
            # generic get
            group_by_attr = row.get(group_by)

            # wrap possible mapping dict
            row_dict = dict(row)

            # add new entries; standardize 
            #aggregates = {}
            #for agg_name in agg_on_names:
            #    aggregates[agg_name] = []
            #row_dict['aggregates'] = aggregates
            row_dict['aggregates'] = []

            indexes = {}
            for index_name in index_on_names:
                indexes[index_name] = {}
            row_dict['indexes'] = indexes

            if group_by_attr is None:
                continue

            if group_by_attr not in group_by_idx:
                group_by_idx[group_by_attr] = row_dict

            # actually include all agg cols, even if None, so agg array indexes align
            agg_dict = {
                agg_on_names[i] : row.get(agg_col)
                for i, agg_col in enumerate(agg_on)
            }

            #aggregates = group_by_idx[group_by_attr]['aggregates']
            #for agg_key, agg_val in agg_dict.items():
            #    aggregates[agg_key].append(agg_val)
            aggregates = group_by_idx[group_by_attr]['aggregates']
            aggregates.append(agg_dict)

            indexes = group_by_idx[group_by_attr]['indexes']
            for i, index_col in enumerate(index_on):
                index_name = index_on_names[i]
                indexes[index_name][row[index_col]] = agg_dict

        if return_index:
            return group_by_idx

        return list(group_by_idx.values())

class CacheBlock:
    '''
    Wraps up a set of query parameters for a specific entity, and provides cached access
    to different types of "re-queries" via an associated Indexer.

    .. admonition:: Additional details

        The goal here is to help build/define entities as the possibly complex
        transformations on the base schema that they are. For example, the Note primitive
        (entity) incorporates details across ``files``, ``notes``, ``note_conversions``,
        and ``note_conversion_matter`` tables (defined in a single endpoint by a
        Composer), often needs to be selected in particular ways (via an Accessor), and
        results stored for fast access later on (handled by an Indexer). This pipeline can
        be daunting and requires too many moving parts to be handled explicitly
        everywhere. CacheBlocks wrap up a set of query "preferences," exposing a simpler
        interface for downstream access to entities. It still allows for low-level control
        over re-grouping/indexing, raw hits to the actual DB, etc, but keeps things
        tighter and well-behaved for the Indexer.
        
        You can think of these as the Indexer's "fingers"; they're deployable mini-Indexes
        that "send back" results to the class cache, which is "broadcast" to all other
        instances for use when necessary.

    .. admonition:: Example usage
        
        .. code-block:: python

            cb = CacheBlock()

            # Set up cached queries with chained params or via call:
            
            cb.where(t.notes.c.name=="name").group_by(t.note_conversions.c.format)
            cb() # get results

            # - OR - # (use strings when known)

            cb.where(t.notes.c.name=="name").group_by('format')
            cb() # get results

            # - OR - # (use kwargs in the call; results returned right away)

            cb(
                where=(t.notes.c.name=="name"),
                group_by='format'
            )
    '''
    def __init__(
        self,
        indexer,
        table,

        cols        = None,
        where       = None,
        distinct_on = None,
        order_by    = None,
        limit       = 0,

        group_by    = None,
        agg_on      = None,
        index_on    = None,
    ):
        self.indexer = indexer

        self.query_args = {
            'table'       : table,
    
            'cols'        : cols,
            'where'       : where,
            'distinct_on' : distinct_on,
            'order_by'    : order_by,
            'limit'       : limit,
    
            'group_by'    : group_by,
            'agg_on'      : agg_on,
            'index_on'    : index_on,
        }

    def _query(self, **kwargs):
        '''Make cached query with defaults, override with those provided'''
        return self.indexer.cached_query(**{
            k : (v if k not in kwargs else kwargs[k])
            for k,v in self.query_args.items()
        })

    def __call__(self, **kwargs):
        '''
        TODO: overload this for the queries, i.e. getting keys or returning aggregates
        '''
        return self._query(**kwargs)

    def where(self, where):
        self.query_args['where'] = where
        return self
        #return self._query(where=where)

    def distinct_on(self, distinct_on):
        self.query_args['distinct_on'] = distinct_on
        return self
        #return self._query(distinct_on=distinct_on)

    def order_by(self, order_by):
        self.query_args['order_by'] = order_by
        return self
        #return self._query(order_by=order_by)

    def limit(self, limit):
        self.query_args['limit'] = limit
        return self

    def group_by(self, group_by):
        self.query_args['group_by'] = group_by
        return self
        #return self._query(group_by=group_by)
    

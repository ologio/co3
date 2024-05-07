from co3.indexer import Indexer


class RelationalIndexer(Indexer):
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

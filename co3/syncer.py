import time
import random
import logging
from collections import defaultdict

from tqdm import tqdm
from colorama import Fore, Back, Style

from co3.differ import Differ
from co3.util.types import Equatable
from co3.util.generic import text_mod


logger = logging.getLogger(__name__)

class Syncer[E: Equatable]:
    def __init__(self, differ: Differ[E]):
        self.differ = differ

        self.should_exit = False

    def handle_l_excl(self, key: E, val: list):
        return key

    def handle_r_excl(self, key: E, val: list):
        return key

    def handle_lr_int(self, key: E, val: tuple[list, list]):
        return key

    def filter_diff_sets(self, l_excl, r_excl, lr_int):
        return l_excl, r_excl, lr_int

    def process_chunk(self, handler_results):
        return handler_results

    def shutdown(self):
        self.should_exit = True

    def _handle_chunk_items(self, membership_items):
        results = []
        for membership, item in membership_items:
            if membership == 0b10:
                res = self.handle_l_excl(*item)
            elif membership == 0b01:
                res = self.handle_r_excl(*item)
            elif membership == 0b11:
                res = self.handle_lr_int(*item)
            else:
                logger.debug('Incorrect membership for chunk item, skipping')
                continue

            results.append(res)

        return results

    def sync(
        self,
        l_select_kwargs: dict,
        r_select_kwargs: dict,
    ) -> list:
        return self.chunked_sync(l_select_kwargs, r_select_kwargs)

    def chunked_sync(
        self,
        l_select_kwargs : dict,
        r_select_kwargs : dict,
        chunk_time      : int | None = None,
        item_limit      : int | None = None,
        chunk_cap       : int | None = None,
    ) -> list:
        '''
        Sync diff sets through attached handlers in chunks.

        Chunks sizes are determined by a *desired processing duration*, i.e., how long
        should be spent aggregating items in handlers (``handle_*`` methods) and the
        subsequent call to ``process_chunk``. This is particularly useful for database
        driven interactions, where one needs wants to embrace bulk insertion (saving on
        repetitive, costly I/O-bound tasks) while performing intermittent consistency
        checks ("saving progress" along the way by inserting in batches; one could imagine
        otherwise performing a huge amount of computation only to encounter an error
        interacting with the database and subsequently rollback the transaction, losing
        all results).

        Chunk *times* are provided (rather than directly specifying sizes) due to the
        variable nature of handlers and the items they need to process. For example, if
        handlers prepare events for bulk submission to an ``execlog.Router``, it's
        difficult to estimate how long the resulting execution traces will take to
        complete. A regular file may take 0.01s to finish preparing, whereas an audio file
        may kick off a 5 minute transcription job. Targeting the aggregate chunk
        processing time allows us to dynamically readjust batch sizes as more jobs
        complete.

        A few extra technical remarks:

        - We randomly shuffle the input items to even out any localized bias present in
          the order of items to be processed. This helps facilitate a more stable estimate
          of the average chunk duration and promote ergodicity ("typical" batch more
          representative of the "average" batch).
        - We employ an exponential moving average over times of completed chunks, more
          heavily weighting the durations of recently completed chunks when readjusting
          sizes. This is not because recent jobs reveal more about the average job length
          (we randomize these as mentioned above), but instead to reflect short-term
          changes in stationarity due to variable factors like resource allocation. For
          example, we'd like to quickly reflect the state of the CPU's capacity after the
          start/end of external processing tasks so we might more accurately estimate
          batch sizes.

        Note: 
            Could be dangerous if going from fast file processing to note processing.
            Imagine estimating 1000 iter/sec, then transferring that to the next batch
            when it's more like 0.2 iter/sec. We would lose any chunking. (Luckily, in
            practice, turns out that notes are almost always processed before the huge set
            of nested files lurking and we don't experience this issue.)

        
        .. admonition:: Sync strategy

            1. Compute diffs under the provided Differ between supported selection sets
               for its SelectableResources.
            2. Perform any additional filtering of the diff sets with
               ``filter_diff_sets``, producing the final set triplet.
            3. Begin a chunked processing loop for all items involved in the final diff
               sets. Items exclusive to the left resource are passed to ``handle_l_excl``,
               ``handle_r_excl`` for those exclusive to the right resource, and
               ``handle_lr_int`` for the left-right intersection. Note how this mechanism
               should inform the implementation of these methods for inheriting
               subclasses: items are only handled once they are part of an actively
               processed chunk. We don't first globally process the sets and "clean up"
               chunk-by-chunk, as this is less stable and can waste compute if we need to
               exit early.

        Parameters:
            chunk_time: desired processing time per batch, in seconds
            item_limit: number of items to sync before exiting
            chunk_cap:  upper limit on the number of items per chunk. Even if never
                        reached, setting this can help prevent anomalous, unbounded time
                        estimates that yield prohibitively sized chunks.
        '''
        # calculate diff and filter diff sets
        l_excl, r_excl, lr_int = self.filter_diff_sets(
            *self.differ.diff(
                l_select_kwargs,
                r_select_kwargs,
            )
        )

        # group items by "intuitive" identifiers
        items = []
        items.extend([(0b10, l) for l in l_excl.items()])
        items.extend([(0b01, r) for r in r_excl.items()])
        items.extend([(0b11, i) for i in lr_int.items()])
 
        # check for empty items
        if not items:
            logger.info('Sync has nothing to do, exiting')
            return []

        # mix items for ergodicity
        random.shuffle(items)

        # if item limit not set, set to all items
        if item_limit is None:
            item_limit = len(items)

        # if chunk cap not set, allow it to be the item limit
        if chunk_cap is None:
            chunk_cap = item_limit
        
        # chunk cap may be large, but max it out at the item limit
        chunk_cap = max(chunk_cap, 1) # ensure >= 1 to avoid infinite loop
        chunk_cap = min(chunk_cap, item_limit)

        # if chunk time not set, set to the largest value: the chunk cap
        if chunk_time is None:
            chunk_time = 0
            chunk_size = chunk_cap
        else:
            # otherwise, assume 1s per item up to the cap size
            chunk_size = max(min(chunk_time, chunk_cap), 1)

        # chunk loop variables
        chunk_timer = 0   
        chunk_report = defaultdict(int) 
        chunk_results = []

        # bar variable tracking remaining items
        remaining = item_limit 
        pbar = tqdm(
            desc=f'Adaptive chunked sync [limit {item_limit}]',
            total=remaining,
        )

        with pbar as _:
            while remaining > 0 and not self.should_exit:
                time_pcnt = chunk_timer/chunk_time*100 if chunk_time else 100
                pbar.set_description(
                    f'Adaptive chunked sync [size {chunk_size} (max {chunk_cap})] '
                    f'[prev chunk {chunk_timer:.2f}s/{chunk_time}s ({time_pcnt:.2f}%)]'
                )

                # time the handler & processing sequence
                chunk_time_start = time.time()

                start_idx = item_limit - remaining
                chunk_items = items[start_idx:start_idx+chunk_size]
                handler_results = self._handle_chunk_items(chunk_items)
                chunk_results.extend(self.process_chunk(handler_results))

                chunk_timer = time.time() - chunk_time_start

                # populate the aggregate chunk report
                chunk_report['size'] += chunk_size
                chunk_report['timer'] += chunk_timer
                chunk_report['count'] += 1

                # remove the number of processed items from those remaining
                remaining -= chunk_size
                pbar.update(n=chunk_size)

                # re-calculate the chunk size with a simple EMA
                s_per_item = chunk_timer / chunk_size
                new_target = chunk_time / s_per_item
                chunk_size = int(0.5*new_target + 0.5*chunk_size)

                # apply the chunk cap and clip by remaining items
                chunk_size = min(min(chunk_size, chunk_cap), remaining)

                # ensure chunk size is >= 1 to prevent loop stalling
                chunk_size = max(chunk_size, 1)

        if self.should_exit:
            logger.info(text_mod('Syncer received interrupt, sync loop exiting early', Fore.BLACK, Back.RED))
                
        avg_chunk_size = chunk_report['size'] / max(chunk_report['count'], 1)
        avg_chunk_time = chunk_report['timer'] / max(chunk_report['count'], 1)
        avg_time_match = avg_chunk_time / chunk_time if chunk_time else 1

        report_text = [
            f'->  Total chunks          : {chunk_report["count"]}              ',
            f'->  Total items processed : {chunk_report["size"]} / {item_limit}',
            f'->  Total time spent      : {chunk_report["timer"]:.2f}s         ',
            f'->  Average chunk size    : {avg_chunk_size:.2f}                 ',
            f'->  Average time/chunk    : {avg_chunk_time:.2f}s / {chunk_time}s',
            f'->  Average time match    : {avg_time_match*100:.2f}%            ',
        ]

        pad = 50
        color_args = []

        logger.info(text_mod('Sync report', Style.BRIGHT, Fore.WHITE, Back.BLUE, pad=pad))
        for line in report_text:
            logger.info(text_mod(line, *color_args, pad=pad))
            
        return chunk_results

from collections import defaultdict
import logging
import pickle
import time
from pathlib import Path

import numpy as np
from tqdm import tqdm
import sqlalchemy as sa

from co3.manager import Manager
from co3 import util


logger = logging.getLogger(__name__)

class VSSManager(Manager):
    def __init__(self, database, engine=None):
        super().__init__(database, engine)

        from localsys.db.databases.core import CoreDatabase

        self.core_db = CoreDatabase()

    def recreate(self):
        start = time.time()

        chunks    = []
        chunk_ids = []
        block_map = {}
        for note_name, note_groupby in self.core_db.blocks().items():
            blocks = note_groupby.get('aggregates', [])
            for block in blocks:
                if block.get('format') != 'src': continue

                block_name = block.get('name', '')
                block_content = block.get('content', '')
                block_map[block_name] = block_content

                block_chunks, chunk_ranges = utils.embed.split_block(block_content)

                chunks.extend(block_chunks)
                chunk_ids.extend([f'{block_name}@{r[0]}:{r[1]}' for r in chunk_ranges])

        logger.info(f'VSS pre-index SELECT took {time.time()-start:.2f}s')
        start = time.time()
    
        # one-time batched embedding
        chunk_embeddings = self.database.access.embed_chunks(chunks)

        logger.info(f'VSS embeddings took {time.time()-start:.2f}s')
        start = time.time()

        # aggregate embeddings up the content hierarchy
        zero_embed = lambda: np.atleast_2d(
            np.zeros(self.database.access._embedding_size, dtype='float32')
        )
        block_embedding_map = defaultdict(zero_embed)
        note_embedding_map  = defaultdict(zero_embed)

        for i, chunk_id in enumerate(chunk_ids):
            at_split   = chunk_id.split('@')
            note_name  = at_split[0]
            block_name = '@'.join(at_split[:3])

            block_embedding_map[block_name] += chunk_embeddings[i]
            note_embedding_map[note_name]   += chunk_embeddings[i]

        # re-normalize aggregated embeddings
        # > little inefficient but gobbled up asymptotically by chunk processing
        # > could o/w first convert each map to a full 2D np array, then back
        for embed_map in [block_embedding_map, note_embedding_map]:
            for k, agg_embedding in embed_map.items():
                embed_map[k] = utils.embed.unit_row_wise(agg_embedding)

        logger.info(f'VSS recreate: pre-index SELECT took {time.time()-start:.2f}s')

        # cols = ['rowid', 'chunk_embedding']
        # def pack_inserts(embeddings):
        #     return [
        #         { cols[0]: i, cols[1]: embedding.tobytes() }
        #         for i, embedding in enumerate(embeddings)
        #     ]

        # chunk_inserts = pack_inserts(chunk_embeddings)
        # block_inserts = pack_inserts(block_embedding_map.values())
        # note_inserts  = pack_inserts(note_embedding_map.values())
            
        chunk_ids = chunk_ids
        block_ids = list(block_embedding_map.keys())
        note_ids  = list(note_embedding_map.keys())

        block_embeddings = np.vstack(list(block_embedding_map.values()))
        note_embeddings  = np.vstack(list(note_embedding_map.values()))

        blocks = [block_map[b] for b in block_ids]
        notes  = note_ids

        self.database.access.write_embeddings({
            'chunks': (chunk_ids, chunk_embeddings, chunks),
            'blocks': (block_ids, block_embeddings, blocks),
            'notes' : (note_ids,  note_embeddings,  notes),
        })

        logger.info(f'Post-embedding parsing took {time.time()-start:.2f}s')

        # utils.db.create_vss0(
        #     self.engine,
        #     'chunks',
        #     columns=list(cols),
        #     inserts=chunk_inserts,
        #     populate=True,
        #     reset=True,
        #     embedding_size=self._embedding_size,
        # )

    def update(self): pass

    def sync(self): pass

    def migrate(self): pass

    def _archive_note_embed(self):
        for block in blocks:
            content = block.get('content')
            if not content: continue

            block_chunks = utils.embed.split_block(content)
            chunks.extend(block_chunks)
            
        embeddings = utils.embed.embed_chunks(chunks)
        block_embeddings = utils.embed.map_groups(embeddings, embed_block_map)

        self.convert_map['vect'] = block_embeddings

        note_embedding = utils.embed.unit_row_wise(
            embeddings.sum(axis=0).reshape(1, -1)
        )

        return {
            'content': note_embedding.tobytes(),
        }

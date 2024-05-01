import time
import pickle
import logging
from pathlib import Path

import sqlalchemy as sa

from co3.accessor import Accessor


logger = logging.getLogger(__name__)

class VSSAccessor(Accessor):
    def __init__(self, cache_path):
        super().__init__()

        self._model      = None
        self._embeddings = None

        self._embedding_size = 384
        self.embedding_path = Path(cache_path, 'embeddings.pkl')

    def write_embeddings(self, embedding_dict):
        self.embedding_path.write_bytes(pickle.dumps(embedding_dict))

    def read_embeddings(self):
        if not self.embedding_path.exists():
            logger.warning(
                f'Attempting to access non-existent embeddings at {self.embedding_path}'
            )
            return None

        return pickle.loads(self.embedding_path.read_bytes())

    @property
    def model(self):
        if self._model is None:
            # model trained with 128 token seqs
            self._model = SentenceTransformer('sentence-transformers/all-MiniLM-L12-v2')
        return self._model

    @property
    def embeddings(self):
        if self._embeddings is None:
            self._embeddings = self.read_embeddings()
        return self._embeddings

    def embed_chunks(self, chunks, batch_size=64, show_prog=True):
        return self.model.encode(
            chunks,
            batch_size           = batch_size,
            show_progress_bar    = show_prog,
            convert_to_numpy     = True,
            normalize_embeddings = True
        )

    def select(
        self,
        connection,
        index_name : str,
        query      : str,
        limit      : int = 10,
        score_threshold = 0.5,
    ):
        '''
        Parameters:
            index_name: one of ['chunks','blocks','notes']
        '''
        if not query:
            return None

        if index_name not in self.embeddings:
            logger.warning(
                f'Index "{index_name}" does not exist'
            )
            return None

        start = time.time()

        query_embedding = self.embed_chunks(query, show_prog=False)
        index_ids, index_embeddings, index_items = self.embeddings[index_name]

        hits = util.semantic_search(
            query_embedding,
            index_embeddings,
            top_k=limit,
            score_function=util.dot_score
        )[0]

        hits = [hit for hit in hits if hit['score'] >= score_threshold]

        for hit in hits:
            idx               = hit['corpus_id']
            hit['group_name'] = index_ids[idx]
            hit['item']       = index_items[idx]

        logger.info(f'{len(hits)} hits in {time.time()-start:.2f}s')

        return hits


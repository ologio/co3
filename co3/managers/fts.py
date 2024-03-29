from collections import defaultdict
import logging
import time

from tqdm import tqdm
import sqlalchemy as sa

from co3 import util

from co3.manager import Manager
from co3.accessors.sql import SQLAccessor


logger = logging.getLogger(__name__)

class FTSManager(Manager):
    def __init__(self, database, engine=None):
        super().__init__(database, engine)

        self.accessor = SQLAccessor(engine)
        self.composer = CoreComposer()

    def recreate_simple(self):
        select_cols = [
            tables.files.c.rpath, tables.files.c.ftype,
            
            tables.notes.c.title, tables.notes.c.link,

            tables.note_conversion_matter.c.type,
            
            tables.blocks.c.name, tables.blocks.c.header,
            tables.blocks.c.elem_pos,
            
            tables.block_conversion_matter.c.content,

            tables.links.c.target,
            #tables.links.c.target.label('link_target'),
        ]

        inserts, res_cols = self.accessor.select(
            'leftward_link_conversions',
            cols=select_cols,
            include_cols=True,
        )

        util.db.populate_fts5(
            self.engine,
            ['search'],
            #columns=select_cols,
            columns=res_cols,
            inserts=inserts,
        )

    def recreate_pivot(self):
        block_cols = [
            tables.files.c.rpath, tables.files.c.ftype,
            
            tables.notes.c.title, tables.notes.c.link,

            tables.note_conversion_matter.c.type,
            
            tables.blocks.c.name, tables.blocks.c.header,
            tables.blocks.c.elem_pos,
            
            #tables.block_conversion_matter.c.content,

            #tables.links.c.target,

            #tables.links.c.target.label('link_target'),
        ]

        lcp = self.accessor.link_content_pivot(cols=block_cols)

        start = time.time()
        inserts, cols = self.accessor.select(
            lcp,
            #cols=select_cols
            include_cols=True,
        )
        logger.info(f'FTS recreate: pre-index SELECT took {time.time()-start:.2f}s')

        return inserts

        #util.db.populate_fts5(
        #    self.engine,
        #    ['search'],
        #    #columns=select_cols,
        #    columns=res_cols,
        #    inserts=inserts,
        #)

    def recreate(self):
        select_cols = [
            tables.links.c.id,

            tables.files.c.rpath, tables.files.c.ftype,

            tables.files.c.title, tables.files.c.link,

            tables.notes.c.type,

            tables.elements.c.name, tables.blocks.c.header,
            tables.elements.c.elem_pos,

            tables.element_conversions.c.format,
            tables.element_conversions.c.content,

            tables.links.c.target,
            tables.links.c.type.label('link_type'),
            #tables.links.c.target.label('link_target'),
        ]

        start = time.time()

        fts_basis = self.composer.get_table('leftward_link_conversions')
        inserts = self.accessor.select(
            fts_basis,
            cols=select_cols,
        )
        logger.info(f'FTS recreate: pre-index SELECT took {time.time()-start:.2f}s')

        start = time.time()
        flattened_blocks = defaultdict(lambda:defaultdict(lambda:None))
        other_results = []

        # flatten conversions across supported formats
        # i.e. put "src" and "html" into columns within a single row
        for row in inserts:
            name = row['name']
            
            # remove pure identifier rows
            link_id = row.pop('id')
            _format = row.pop('format')
            content = row.pop('content')
            
            # add new derived rows
            row['src']  = None
            row['html'] = None
            
            if name is None:
                other_results.append(row)
                continue
            
            if flattened_blocks[name][link_id] is None:
                flattened_blocks[name][link_id] = row
                
            nrow = flattened_blocks[name][link_id]
            if _format == 'src':
                 nrow['src'] = content
            elif _format == 'html5':
                 nrow['html'] = content
                    
        inserts = other_results
        for name, link_dict in flattened_blocks.items():
            for link_id, row in link_dict.items():
                inserts.append(row)

        # should have at least one result to index
        assert len(inserts) > 0
        cols = inserts[0].keys()

        # all keys should match
        assert all([cols == d.keys() for d in inserts])

        logger.info(f'FTS recreate: insert post-processing took {time.time()-start:.2f}s')

        util.db.populate_fts5(
            self.engine,
            ['search'],
            columns=list(cols),
            inserts=inserts,
        )

    def update(self): pass

    def sync(self): pass

    def migrate(self): pass



def recreate_old(self):
    full_table = schema.leftward_block_conversions()
    primitive_tables = schema.primitive_tables() 

    virtual_inserts = []
    for ptype, table in primitive_tables.items():
        table_rows = db.util.sa_exec_dicts(self.engine, sa.select(table))
        for row in table_rows:
            pdict = util.db.prepare_insert(full_table, row)
            pdict['ptype'] = ptype

            virtual_inserts.append(pdict)

    select_cols = [
        tables.blocks.c.name, tables.blocks.c.content,
        tables.blocks.c.html, tables.blocks.c.header,
        tables.blocks.c.block_pos, tables.blocks.c.note_name,
        
        tables.links.c.to_file,
        
        tables.notes.c.link,
        
        tables.note_conversion_matter.c.type,
        tables.note_conversion_matter.c.summary,

        'ptype',
    ]
        
    util.db.populate_fts5(
        self.engine,
        'search',
        columns=select_cols,
        inserts=virtual_inserts,
    )
        

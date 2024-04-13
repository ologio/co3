import sqlalchemy as sa

from co3.engine import Engine


class SQLEngine(Engine):
    def __init__(self, url: str | sa.URL, **kwargs):
        super().__init__(url, **kwargs)

    def _create_manager(self):
        return sa.create_engine(*self.manager_args, self.manager_kwargs)
        
    @contextmanager
    def connect(self, timeout=None):
        return self.manager.connect()
    
    @staticmethod
    def _execute(
        connection,
        statement,
        bind_params=None,
        include_cols=False,
    ):
        '''
        Execute a general SQLAlchemy statement, optionally binding provided parameters and
        returning associated column names.

        Parameters:
            connection:   database connection instance
            statement:    SQLAlchemy statement
            bind_params: 
            include_cols: whether to return
        '''
        res = connection.execute(statement, bind_params)

        if include_cols:
            cols = list(res.mappings().keys())
            return res, cols

        return res

    @staticmethod
    def _exec_explicit(connection, statement, bind_params=None):
        trans = connection.begin()  # start a new transaction explicitly
        try:
            result = connection.execute(statement, bind_params)
            trans.commit()  # commit the transaction explicitly
            return result
        except:
            trans.rollback()  # rollback the transaction explicitly
            raise

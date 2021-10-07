import sqlalchemy as sa

from .stmt_text import _insert_query_params


class QueryCounter:
    """ Counts the number of queries """

    def __init__(self, engine: sa.engine.Engine):
        super(QueryCounter, self).__init__()
        self.engine = engine
        self.n = 0

    def start_logging(self):
        sa.event.listen(self.engine, "after_cursor_execute", self._after_cursor_execute_event_handler, named=True)

    def stop_logging(self):
        sa.event.remove(self.engine, "after_cursor_execute", self._after_cursor_execute_event_handler)
        self._done()

    def _done(self):
        """ Handler executed when logging is stopped """

    def _after_cursor_execute_event_handler(self, **kw):
        self.n += 1

    def print_log(self):
        pass  # nothing to do

    # Context manager

    def __enter__(self):
        self.start_logging()
        return self

    def __exit__(self, *exc):
        self.stop_logging()
        if exc != (None, None, None):
            self.print_log()
        return False


class QueryLogger(QueryCounter, list):
    """ Log raw SQL queries on the given engine """

    def _after_cursor_execute_event_handler(self, **kw):
        super()._after_cursor_execute_event_handler()
        # Compile, append
        self.append(_insert_query_params(kw['statement'], kw['parameters']))

    def print_log(self):
        for i, q in enumerate(self):
            print('=' * 5, ' Query #{}'.format(i))
            print(q)


class ExpectedQueryCounter(QueryLogger):
    """ A QueryLogger that expects a certain number of queries, raises an error otherwise """

    def __init__(self, engine: sa.engine.Engine, expected_queries: int, comment: str):
        super().__init__(engine)
        self.expected_queries = expected_queries
        self.comment = comment

    def _done(self):
        if self.n != self.expected_queries:
            self.print_log()
            raise AssertionError('{} (expected {} queries, actually had {})'
                                 .format(self.comment, self.expected_queries, self.n))

from contextlib import contextmanager

@contextmanager
def splitgraph_transaction():
    from splitgraph.engine import _ENGINES

    try:
        yield
        [e.commit() for e in _ENGINES.values()]
    except:
        [e.rollback() for e in _ENGINES.values()]
        raise
    finally:
        [e.close() for e in _ENGINES.values()]
        
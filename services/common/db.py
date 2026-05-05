from contextlib import contextmanager
import psycopg

from .config import settings


@contextmanager
def get_conn():
    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            yield conn, cur

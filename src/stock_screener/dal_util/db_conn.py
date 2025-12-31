import psycopg2
import psycopg2.pool
import threading
import logging

from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


class DatabaseConnection:
    _instance = None
    _pool = None
    _lock = threading.Lock()

    def __new__(cls, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseConnection, cls).__new__(cls)
                if kwargs:
                    try:
                        cls._pool = psycopg2.pool.ThreadedConnectionPool(
                            minconn=1,
                            maxconn=10,
                            cursor_factory=RealDictCursor,
                            **kwargs
                        )
                        logger.info("Database connection pool initialized.")
                    except Exception as e:
                        logger.error(f"Failed to initialize database connection pool: {e}")
                        raise
            return cls._instance

    @classmethod
    def get_connection(cls) -> psycopg2.extensions.connection:
        if not cls._pool:
            raise RuntimeError("Database connection pool not initialized. "
                                "Call DatabaseConnection() with connection parameters first.")
        try:
            conn = cls._pool.getconn()
            logger.debug("Retrieved connection from pool.")
            return conn
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise

    @classmethod
    def close_connection(cls) -> None:
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            cls._instance = None
            logger.info("Database connection pool closed.")
        else:
            logger.warning("No connection pool to close.")
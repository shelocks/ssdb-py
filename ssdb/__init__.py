from ssdb.client import (
    SSDB,
    SSDBResponse,
    ConnectionError,
    Connection,
    ConnectionPool
    )

__version__ = '1.0.0'
VERSION = tuple(map(int, __version__.split('.')))

__all__ = [
    'SSDB', 'SSDBResponse', 'ConnectionPool', 'Connection',
    'ConnectionError'
]

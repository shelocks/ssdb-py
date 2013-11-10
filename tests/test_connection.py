from ssdb.client import ConnectionPool, Connection, ConnectionError
from unittest import TestCase
import unittest


class ConnectionTest(TestCase):
    def get_pool(self, max_connections=1):
        pool = ConnectionPool(max_connections=max_connections)
        return pool

    def test_multiple_connections(self):
        pool = self.get_pool(2)

        c1 = pool.get_connection()
        c2 = pool.get_connection()

        self.assertTrue(c1 != c2)

    def test_too_many_connection(self):
        pool = self.get_pool()

        c1 = pool.get_connection()

        self.assertRaises(ConnectionError, pool.get_connection)

    def test_release_connection(self):
        pool = self.get_pool()

        c1 = pool.get_connection()

        self.assertRaises(ConnectionError, pool.get_connection)

        pool.release(c1)

        c2 = pool.get_connection()

        self.assertEqual(c1, c2)


if __name__ == '__main__':
    unittest.main()

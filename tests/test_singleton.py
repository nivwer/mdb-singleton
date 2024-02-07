import unittest
from concurrent.futures import ThreadPoolExecutor, wait

from mdb_singleton.singleton import MongoDBSingleton
import mdb_singleton.settings as MDBS

MDBS.LOGGING_ENABLED = False


class TestMongoDBSingleton(unittest.TestCase):

    def tearDown(self):
        """
        Close connections of MongoDBSingleton.
        """
        for connection in MongoDBSingleton._connections.values():
            if connection.client:
                connection.client.close()

        MongoDBSingleton._connections = {}

    def test_create_connection(self):
        """
        When creating a new instance, a connection to MongoDB should be created and ready to use.
        """

        connection = MongoDBSingleton()
        key = connection.key

        # Get instances of connections stored in the MongoDBSingleton class
        connections: dict = MongoDBSingleton._connections
        keys = list(connections.keys())

        msg = "The connection instance must belong to the MongoDBSingleton class."
        self.assertIsInstance(connection, MongoDBSingleton, msg)

        msg = "The connection instance must be stored in the MongoDBSingleton class."
        self.assertIn(key, keys, msg)

        msg = "The connection to MongoDB should be ready to use."
        self.assertTrue(connection.client, msg)

        msg = f"The number of connections ({len(connections)}) must be equal to 1."
        self.assertEqual(len(connections), 1, msg)

    def test_close_connection(self):
        """
        When closing an connection instance, it should be removed from the MongoDBSingleton class.
        """

        connection = MongoDBSingleton()
        key = connection.key

        connections_before = MongoDBSingleton._connections
        keys_before = list(connections_before.keys())

        msg = "The connection instance must be stored in the MongoDBSingleton class"
        self.assertIn(key, keys_before, msg)

        MongoDBSingleton.close_connection(key=key)

        connections_after = MongoDBSingleton._connections
        keys_after = list(connections_after.keys())

        msg = "The connection instance must not be stored in the MongoDBSingleton class after closing"
        self.assertNotIn(key, keys_after)

        msg = f"The number of connections ({len(connections_after)}) must be equal to 0."
        self.assertEqual(len(connections_after), 0, msg)

    def test_connection_per_threads(self):
        """
        Each thread should create a connection instance, and the total number of connections
        should match the number of threads.
        """

        threads = 3
        operations = 9
        operations_count = 0

        executor = ThreadPoolExecutor(max_workers=threads)

        futures = []

        for o in range(operations):
            future = executor.submit(MongoDBSingleton)
            operations_count += 1
            futures.append(future)

        wait(futures)

        connections: dict = MongoDBSingleton._connections

        msg = f"The number of connections ({len(connections)}) must be equal to the number of threads ({threads})"
        self.assertEqual(len(connections), threads, msg)

        msg = f"The total number of operations ({operations_count}) must match the expected operations ({operations})"
        self.assertEqual(operations_count, operations, msg)

    def test_close_all_connections(self):
        """
        The close_all_connections method of MongoDBSingleton to ensure that all connections are properly closed.
        """

        threads = 3
        operations = 9

        executor = ThreadPoolExecutor(max_workers=threads)

        futures = []

        for o in range(operations):
            future = executor.submit(MongoDBSingleton)
            futures.append(future)

        wait(futures)

        connections_before: dict = MongoDBSingleton._connections

        msg = f"The number of connections ({len(connections_before)}) must be equal to the number of threads ({threads})"
        self.assertEqual(len(connections_before), threads, msg)

        MongoDBSingleton.close_all_connections()

        connections_after: dict = MongoDBSingleton._connections

        msg = f"The number of instances ({len(connections_after)}) must be equal to 0."
        self.assertEqual(len(connections_after), 0, msg)

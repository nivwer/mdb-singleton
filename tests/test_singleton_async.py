import unittest
import asyncio

from mdb_singleton.singleton import MongoDBSingleton, MongoDBSingletonAsync
import mdb_singleton.settings as MDBS

MDBS.LOGGING_ENABLED = False


class TestMongoDBSingletonAsync(unittest.IsolatedAsyncioTestCase):

    def tearDown(self):
        """
        Close connections and clean storage of MongoDBSingleton instances.
        """
        for connection in MongoDBSingleton._connections.values():
            if connection.client:
                connection.client.close()

        MongoDBSingleton._connections = {}

    async def create_connection(self):
        connection = MongoDBSingletonAsync()
        return connection

    async def test_create__connection(self):
        """
        When creating a new instance, a connection to MongoDB should be created and ready to use.
        """

        tasks = [self.create_connection()]
        result = await asyncio.gather(*tasks)

        connection = result[0]
        key = connection.key

        # Get instances of connections stored in the MongoDBSingleton class
        connections: dict = MongoDBSingleton._connections
        keys = list(connections.keys())

        msg = "The connection instance must belong to the MongoDBSingletonAsync class."
        self.assertIsInstance(connection, MongoDBSingletonAsync, msg)

        msg = "The connection instance must be stored in the MongoDBSingleton class."
        self.assertIn(key, keys, msg)

        msg = "The connection to MongoDB should be ready to use."
        self.assertTrue(connection.client, msg)

        msg = f"The number of connections ({len(connections)}) must be equal to 1."
        self.assertEqual(len(connections), 1, msg)

    async def test_close_connection(self):
        """
        When closing an connection instance, it should be removed from the MongoDBSingletonAsync class.
        """

        tasks = [self.create_connection()]
        result = await asyncio.gather(*tasks)

        connection = result[0]
        key = connection.key

        connections_before = MongoDBSingleton._connections
        keys_before = list(connections_before.keys())

        msg = "The connection instance must be stored in the MongoDBSingleton class"
        self.assertIn(key, keys_before, msg)

        MongoDBSingleton.close_connection(key=key)

        connections_after = MongoDBSingleton._connections
        keys_after = list(connections_after.keys())

        msg = (
            "The connection instance must not be stored in the MongoDBSingleton class after closing"
        )
        self.assertNotIn(key, keys_after)

        msg = f"The number of connections ({len(connections_after)}) must be equal to 0."
        self.assertEqual(len(connections_after), 0, msg)

    async def test_connection_per_tasks(self):
        """
        Each task should create a connection instance, and the total number of instances
        should match the number of tasks.
        """
        operations = 9
        operations_count = 0

        tasks = []
        for o in range(operations):
            operations_count += 1
            tasks.append(self.create_connection())

        await asyncio.gather(*tasks)

        connections: dict = MongoDBSingleton._connections

        msg = f"The number of connections ({len(connections)}) must be equal to the number of tasks ({len(tasks)})"
        self.assertEqual(len(connections), len(tasks), msg)

        msg = f"The total number of operations ({operations_count}) must match the expected operations ({operations})"
        self.assertEqual(operations_count, operations, msg)

    async def test_close_all_connections(self):
        """
        The close_all_connections method of MongoDBSingleton to ensure that all connections are properly closed.
        """

        tasks = []
        for o in range(9):
            tasks.append(self.create_connection())

        await asyncio.gather(*tasks)

        connections_before: dict = MongoDBSingleton._connections

        msg = f"The number of connections ({len(connections_before)}) must be equal to the number of tasks ({len(tasks)})"
        self.assertEqual(len(connections_before), len(tasks), msg)

        MongoDBSingleton.close_all_connections()

        connections_after: dict = MongoDBSingleton._connections

        msg = f"The number of instances ({len(connections_after)}) must be equal to 0."
        self.assertEqual(len(connections_after), 0, msg)

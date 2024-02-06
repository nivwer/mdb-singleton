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
        # Close connections for each instance and clean storage of MongoDBSingleton instances.
        for instance in MongoDBSingleton._instances.values():
            if instance.client:
                instance.client.close()

        MongoDBSingleton._instances = {}

    async def create_instance(self):
        instance = MongoDBSingletonAsync()
        return instance

    async def test_create_instance(self):
        """
        When creating a new instance, a connection to MongoDB should be created and ready to use.
        """

        tasks = [self.create_instance()]
        result = await asyncio.gather(*tasks)

        instance = result[0]
        key = instance.key

        # Get instances stored in the MongoDBSingleton class
        instances: dict = MongoDBSingleton._instances
        keys = list(instances.keys())

        msg = "The instance must belong to the MongoDBSingletonAsync class."
        self.assertIsInstance(instance, MongoDBSingletonAsync, msg)

        msg = "The instance must be stored in the MongoDBSingleton class."
        self.assertIn(key, keys, msg)

        msg = "The connection to MongoDB should be ready to use."
        self.assertTrue(instance.client, msg)

        msg = f"The number of instances ({len(instances)}) must be equal to 1."
        self.assertEqual(len(instances), 1, msg)

    async def test_close_instance(self):
        """
        When closing an instance, it should be removed from the MongoDBSingletonAsync class.
        """

        tasks = [self.create_instance()]
        result = await asyncio.gather(*tasks)

        instance = result[0]
        key = instance.key

        instances_before = MongoDBSingleton._instances
        keys_before = list(instances_before.keys())

        msg = "The instance must be stored in the MongoDBSingleton class"
        self.assertIn(key, keys_before, msg)

        MongoDBSingleton.close(key=key)

        instances_after = MongoDBSingleton._instances
        keys_after = list(instances_after.keys())

        msg = "The instance must not be stored in the MongoDBSingleton class after closing"
        self.assertNotIn(key, keys_after)

        msg = f"The number of instances ({len(instances_after)}) must be equal to 0."
        self.assertEqual(len(instances_after), 0, msg)

    async def test_instance_per_tasks(self):
        """
        Each task should create a instance, and the total number of instances
        should match the number of tasks.
        """
        operations = 9
        operations_count = 0

        tasks = []
        for o in range(operations):
            operations_count += 1
            tasks.append(self.create_instance())

        await asyncio.gather(*tasks)

        instances: dict = MongoDBSingleton._instances

        msg = f"The number of instances ({len(instances)}) must be equal to the number of tasks ({len(tasks)})"
        self.assertEqual(len(instances), len(tasks), msg)

        msg = f"The total number of operations ({operations_count}) must match the expected operations ({operations})"
        self.assertEqual(operations_count, operations, msg)

    async def test_close_all_instances(self):
        """
        The close_all method of MongoDBSingleton to ensure that all instances are properly closed.
        """

        tasks = []
        for o in range(9):
            tasks.append(self.create_instance())

        await asyncio.gather(*tasks)

        instances: dict = MongoDBSingleton._instances

        msg = f"The number of instances ({len(instances)}) must be equal to the number of tasks ({len(tasks)})"
        self.assertEqual(len(instances), len(tasks), msg)

        MongoDBSingleton.close_all()

        instances: dict = MongoDBSingleton._instances

        msg = f"The number of instances ({len(instances)}) must be equal to 0."
        self.assertEqual(len(instances), 0, msg)

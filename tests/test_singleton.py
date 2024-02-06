import unittest
from concurrent.futures import ThreadPoolExecutor, wait

from mdb_singleton.singleton import MongoDBSingleton
import mdb_singleton.settings as MDBS

MDBS.LOGGING_ENABLED = False


class TestMongoDBSingleton(unittest.TestCase):

    def tearDown(self):
        """
        Close connections and clean storage of MongoDBSingleton instances.
        """
        # Close connections for each instance and clean storage of MongoDBSingleton instances.
        for instance in MongoDBSingleton._instances.values():
            if instance.client:
                instance.client.close()

        MongoDBSingleton._instances = {}

    def test_create_instance(self):
        """
        When creating a new instance, a connection to MongoDB should be created and ready to use.
        """

        instance = MongoDBSingleton()
        key = instance.key

        # Get instances stored in the MongoDBSingleton class
        instances: dict = MongoDBSingleton._instances
        keys = list(instances.keys())

        msg = "The instance must belong to the MongoDBSingleton class."
        self.assertIsInstance(instance, MongoDBSingleton, msg)

        msg = "The instance must be stored in the MongoDBSingleton class."
        self.assertIn(key, keys, msg)

        msg = "The connection to MongoDB should be ready to use."
        self.assertTrue(instance.client, msg)

        msg = f"The number of instances ({len(instances)}) must be equal to 1."
        self.assertEqual(len(instances), 1, msg)

    def test_close_instance(self):
        """
        When closing an instance, it should be removed from the MongoDBSingleton class.
        """

        instance = MongoDBSingleton()
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

    def test_instance_per_threads(self):
        """
        Each thread should create a instance, and the total number of instances
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

        instances: dict = MongoDBSingleton._instances

        msg = f"The number of instances ({len(instances)}) must be equal to the number of threads ({threads})"
        self.assertEqual(len(instances), threads, msg)

        msg = f"The total number of operations ({operations_count}) must match the expected operations ({operations})"
        self.assertEqual(operations_count, operations, msg)

    def test_close_all_instances(self):
        """
        The close_all method of MongoDBSingleton to ensure that all instances are properly closed.
        """

        threads = 3
        operations = 9

        executor = ThreadPoolExecutor(max_workers=threads)

        futures = []

        for o in range(operations):
            future = executor.submit(MongoDBSingleton)
            futures.append(future)

        wait(futures)

        instances: dict = MongoDBSingleton._instances

        msg = f"The number of instances ({len(instances)}) must be equal to the number of threads ({threads})"
        self.assertEqual(len(instances), threads, msg)

        MongoDBSingleton.close_all()

        instances: dict = MongoDBSingleton._instances

        msg = f"The number of instances ({len(instances)}) must be equal to 0."
        self.assertEqual(len(instances), 0, msg)


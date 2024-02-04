import os
from dotenv import load_dotenv
import logging
import threading
import asyncio
from pymongo import errors, MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:     %(message)s")


class MongoDBConnection:
    """
    MongoDBConnection class provides a base class for establishing and closing connections
    to MongoDB. It supports both synchronous (thread) and asynchronous (asyncio) clients.
    """

    def _initialize_connection(self):
        """
        Internal method to initialize MongoDB connection based on the specified client type.
        """
        MONGO_URI = os.getenv("MONGO_URI")

        try:
            client_class = MongoClient if self.type == "thread" else AsyncIOMotorClient
            self.client = client_class(MONGO_URI)

            msg = f"MongoDB connection established with key: {self.key} ({self.type})"
            logging.info(msg=msg)

        except errors.ServerSelectionTimeoutError as e:
            msg = "MongoDB server selection timeout error: %s"
            logging.error(msg=msg, exc_info=e)

        except errors.ConnectionFailure as e:
            msg = "MongoDB connection error: %s"
            logging.error(msg=msg, exc_info=e)

        except errors.InvalidURI as e:
            msg = "MongoDB Invalid URI error: %s"
            logging.error(msg=msg, exc_info=e)

        except errors.ConfigurationError as e:
            msg = "MongoDB configuration error: %s"
            logging.error(msg=msg, exc_info=e)

    def close_connection(self):
        """
        Close the MongoDB connection if it exists.
        """
        if self.client:
            self.client.close()

            msg = f"MongoDB connection closed for key: {self.key} ({self.type})"
            logging.info(msg=msg)


class MongoDBSingleton(MongoDBConnection):
    """
    MongoDBSingleton class provides a thread-safe singleton pattern for MongoDBConnection.
    It ensures a single MongoDB connection per thread.
    """

    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        Create a new MongoDBConnection instance or return an existing one based on the thread key.
        """
        key = str(id(threading.current_thread()))

        if key not in cls._instances:
            with cls._lock:
                cls._instances[key] = MongoDBConnection().__new__(cls)

                cls._instances[key].key = key
                cls._instances[key].type = "thread"

                cls._instances[key]._initialize_connection()

        return cls._instances[key]

    @classmethod
    def close_all_connections(cls):
        """
        Close all MongoDB connections created by the singleton pattern.
        """
        keys = list(cls._instances.keys())

        for key in keys:
            cls._instances[key].close_connection()
            cls._instances.pop(key)

    @classmethod
    def close(cls, key):
        """
        Close the MongoDB connection associated with the given key.
        """
        keys = list(cls._instances.keys())

        if key in keys:
            cls._instances[key].close_connection()
            cls._instances.pop(key)


class MongoDBSingletonAsync(MongoDBSingleton):
    """
    MongoDBSingletonAsync extends MongoDBSingleton for asynchronous (asyncio) use.
    It ensures a single MongoDB connection per asyncio task.
    """

    def __new__(cls, *args, **kwargs):
        """
        Create a new MongoDBConnection instance or return an existing one based on the task key.
        """
        key = str(id(asyncio.current_task()))

        if key not in cls._instances:
            with cls._lock:
                cls._instances[key] = MongoDBConnection().__new__(cls)

                cls._instances[key].key = key
                cls._instances[key].type = "task"

                cls._instances[key]._initialize_connection()

        return cls._instances[key]

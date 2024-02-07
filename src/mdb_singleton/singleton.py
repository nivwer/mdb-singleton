import os
from dotenv import load_dotenv
import logging
import threading
import asyncio
from pymongo import errors, MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

import mdb_singleton.settings as settings

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
        MONGO_URI: str = os.getenv("MONGO_URI")

        try:
            client_class = MongoClient if self.type == "thread" else AsyncIOMotorClient
            self.client = client_class(MONGO_URI)

            if settings.LOGGING_ENABLED:
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

    def _close_connection(self):
        """
        Close the MongoDB connection if it exists.
        """
        if self.client:
            self.client.close()

            if settings.LOGGING_ENABLED:
                msg = f"MongoDB connection closed for key: {self.key} ({self.type})"
                logging.info(msg=msg)


class MongoDBSingleton(MongoDBConnection):
    """
    MongoDBSingleton class provides a thread-safe singleton pattern for MongoDBConnection.
    It ensures a single MongoDB connection per thread.
    """

    _connections = {}
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        Create a new MongoDBConnection instance or return an existing one based on the thread key.
        """
        key = str(id(threading.current_thread()))

        if key not in cls._connections:
            with cls._lock:
                cls._connections[key] = MongoDBConnection().__new__(cls)

                cls._connections[key].key = key
                cls._connections[key].type = "thread"

                cls._connections[key]._initialize_connection()

        return cls._connections[key]

    @classmethod
    def close_all_connections(cls):
        """
        Close all MongoDB connections created by the singleton pattern.
        """
        keys = list(cls._connections.keys())

        for key in keys:
            cls._connections[key]._close_connection()
            cls._connections.pop(key)

    @classmethod
    def close_connection(cls, key: str):
        """
        Close the MongoDB connection associated with the given key.
        """
        keys = list(cls._connections.keys())

        if key in keys:
            cls._connections[key]._close_connection()
            cls._connections.pop(key)


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

        if key not in cls._connections:
            with cls._lock:
                cls._connections[key] = MongoDBConnection().__new__(cls)

                cls._connections[key].key = key
                cls._connections[key].type = "task"

                cls._connections[key]._initialize_connection()

        return cls._connections[key]

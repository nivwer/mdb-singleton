# MongoDB Singleton

`mdb_singleton` is a Python package designed to optimize MongoDB connection management, avoiding unnecessary opening and closing of connections and efficiently utilizing existing connections. The package focuses on mitigating resource wastage while ensuring a scalable and thread-safe approach.

## Installation

Install the package using pip:

```bash
pip install mdb_singleton
```

## Overview

The primary goal of `mdb_singleton` is to address the challenges associated with MongoDB connection handling in a resource-efficient manner. The package achieves this by generating thread-specific and task-specific singletons, allowing multiple threads or tasks to leverage their dedicated connections concurrently, eliminating bottlenecks.

## Features

- **Thread-Safe Singletons:** `MongoDBSingleton` facilitates the creation of MongoDB connections unique to each thread, preventing contention and ensuring thread safety.
- **Asyncio (ASGI) Support:** `MongoDBSingletonAsync` extends the functionality for asynchronous applications, creating dedicated connections for each asyncio task.
- **Resource Optimization:** `mdb_singleton` eliminates unnecessary connection overhead by reusing existing connections within the context of a thread or asyncio task.

## Usage

### Basic Usage

```python
from mdb_singleton import MongoDBSingleton

# Create or retrieve a MongoDB connection singleton for the current thread
mongo_conn = MongoDBSingleton()
client = mongo_conn.client

# Your MongoDB operations here...

# Avoid closing the connection manually, as the package is designed for reuse.
```

### ASGI (Asyncio) Usage

```python
from mdb_singleton import MongoDBSingletonAsync

# Create or retrieve an async MongoDB connection singleton for the current asyncio task
mongo_conn = MongoDBSingletonAsync()
client = mongo_conn.client

# Your async MongoDB operations here...

# Avoid closing the connection manually, as the package is designed for reuse.
```

### Connection Closure on Application Exit

To ensure proper closure of all MongoDB connections initiated with `MongoDBSingleton` and `MongoDBSingletonAsync` when the program or server terminates, include the following line of code in your startup script, settings.py, or any relevant location:

```python
import atexit
from mdb_singleton import MongoDBSingleton

# Register connection closure on application exit
atexit.register(MongoDBSingleton.close_all_connections)
```

### Manual Closure (Exceptional Cases)

In exceptional cases where manual closure is necessary, it should be done using the following syntax:

```python
from mdb_singleton import MongoDBSingleton

mongo_conn = MongoDBSingleton()
client = mongo_conn.client

# Your MongoDB operations here...

# Close the MongoDB connection manually
MongoDBSingleton.close_connection(mongo_conn.key)
```

This method will close the connection and remove the instance from the active connections.

## Environment Variable

`mdb_singleton` expects the following environment variable to be set for MongoDB connection:

```bash
# .env file
MONGO_URI = "mongodb+srv://..."

```

## Why Use `mdb_singleton`?

- **Efficient Resource Utilization:** Avoids unnecessary opening and closing of connections, optimizing resource usage.
- **Thread and Task Specific:** Generates dedicated connections for each thread or asyncio task, preventing contention and bottlenecks.
- **Scalable Design:** Scales effectively with multi-threaded and asyncio applications, adapting to the demands of concurrent execution.

## License

Copyright © 2024 [nivwer](https://github.com/nivwer).  
This package is licensed under the [MIT License](/LICENSE).  

  

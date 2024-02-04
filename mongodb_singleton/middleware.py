import atexit
from mongodb_singleton.singleton import MongoDBSingleton, MongoDBSingletonAsync


class MongoDBMiddleware:
    """
    MongoDBMiddleware class serves as middleware to manage MongoDB connections in a synchronous web application.
    It uses MongoDBSingleton to ensure a single MongoDB connection per thread.
    """

    def __init__(self, get_response=None, mongodb_singleton=MongoDBSingleton):
        """
        Initialize MongoDBMiddleware instance.
        """
        self.get_response = get_response
        self.mongo_conn = mongodb_singleton()

        # Register connection closure on application exit
        atexit.register(MongoDBSingleton.close_all_connections)

    def __call__(self, request):
        response = self.get_response(request)
        return response


class MongoDBMiddlewareASGI(MongoDBMiddleware):
    """
    MongoDBMiddlewareASGI class extends MongoDBMiddleware for asynchronous (ASGI) web applications.
    It uses MongoDBSingletonAsync to ensure a single MongoDB connection per asyncio task.
    """

    def __init__(self, get_response=None, mongodb_singleton=MongoDBSingletonAsync):
        """
        Initialize MongoDBMiddlewareASGI instance.
        """
        super().__init__(get_response, mongodb_singleton)

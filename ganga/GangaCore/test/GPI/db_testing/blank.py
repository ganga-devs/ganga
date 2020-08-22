import os
import pytest
import pymongo


def test_mongo_running_host():
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError
    PORT = os.environ["MONGODB_PORT"] if "MONGODB_PORT" in os.environ else 27017
    HOST = os.environ["MONGODB_HOST "] if "MONGODB_HOST " in os.environ else "mongodb"
    connection_string = f"mongodb://{HOST}:{PORT}/"
    client = MongoClient(connection_string, serverSelectionTimeoutMS=10, connectTimeoutMS=20000)
    try:
        _ = client.server_info() # Forces a call.
        assert True
    except ServerSelectionTimeoutError:
        assert False

def test_mongo_running():
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError
    client = MongoClient(serverSelectionTimeoutMS=10, connectTimeoutMS=20000)
    try:
        _ = client.server_info() # Forces a call.
        assert True
    except ServerSelectionTimeoutError:
        assert False
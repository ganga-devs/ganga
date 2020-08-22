import os
import pytest
import pymongo

def test_keys():
    """
    Check whether the keys are found in the in the os
    """
    if "MONGODB_HOST" in os.environ.keys() and "MONGODB_PORT" in os.environ.keys():
        assert True
    else:
        assert False

# def test_one():
#     from pymongo import MongoClient
#     from pymongo.errors import ServerSelectionTimeoutError
#     PORT = os.environ["MONGODB_PORT"] if "MONGODB_PORT" in os.environ else 27017
#     HOST = os.environ["MONGODB_HOST "] if "MONGODB_HOST " in os.environ else "localhost"
#     connection_string = f"mongodb://{HOST}:{PORT}/"
#     client = MongoClient(connection_string, serverSelectionTimeoutMS=10, connectTimeoutMS=20000)
#     try:
#         _ = client.server_info() # Forces a call.
#         assert True
#     except ServerSelectionTimeoutError:
#         assert False



def test_two():
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


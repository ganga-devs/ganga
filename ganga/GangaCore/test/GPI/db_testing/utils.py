import os
import pymongo


def clean_database(host, port):
    """
    Clean the information from the database
    """
    db_name = "testDatabase"
    connection_string = f"mongodb://{host}:{port}/"
    _ = pymongo.MongoClient(connection_string)
    _.drop_database(db_name)


def get_db_connection(host, port):
    """
    Connection to the testing mongo database
    """

    # FIXME: Can't seem to get `ganga` to read the modified config changes
    # patching the effect by using custom config
    db_name = "testDatabase"
    connection_string = f"mongodb://{host}:{port}/"
    _ = pymongo.MongoClient(connection_string)
    connection = _[db_name]

    return connection


def get_options(host, port):
    config = [
        ("DatabaseConfigurations", "host", host),
        ("DatabaseConfigurations", "port", port),
        ("DatabaseConfigurations", "baseImage", "mongo"),
        ("DatabaseConfigurations", "dbname", "testDatabase"),
        ("DatabaseConfigurations", "containerName", "testContainer")
    ]
    options = [('TestingFramework', 'AutoCleanup', 'False')]
    if "GANGA_GITHUB_HOST" in os.environ.keys():  # we are testing in github actions
        options.append(("DatabaseConfigurations", "controller", "native"))
    else:  # docker is better for local testing
        options.append(("DatabaseConfigurations", "controller", "docker"))
    return options + config


def get_host_port():
    if "GANGA_GITHUB_HOST" in os.environ.keys():  # we are testing in github actions
        return "mongodb", "27017"
    else:  # docker is better for local testing
        return "localhost", "27017"


def getNestedList():
    from GangaCore.GPI import LocalFile, GangaList
    gl = GangaList()
    gl2 = GangaList()
    for i in range(5):
        gl.append(LocalFile())
    for i in range(5):
        gl2.append(gl)
    return gl2
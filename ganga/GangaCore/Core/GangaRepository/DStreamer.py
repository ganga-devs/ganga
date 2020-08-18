import os
import json
import time
import pymongo
import datetime

from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.Plugin import PluginManagerError, allPlugins
from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Base.Proxy import addProxy, isType, stripProxy
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList

logger = getLogger()

def object_to_database(j, document, master, ignore_subs=[]):
    """Save GangaObject in database

    Parameters
    ----------
    j (GangaObject): The object to be stored in the database
    master (int): Index id of the master Job of `j`, if `j` is a subjob
    document (pymongo.document) : The document of database where the object json will be stored
    ignore_subs (list): The attribtutes of the object to be ignored when storing in database
    """
    json_content = j.to_json()

    for sub in ignore_subs:
        json_content.pop(sub, None)

    json_content["modified_time"] = time.time()
    json_content["master"] = master

    #!  Assumption that only job objects are updated after creation, needs clarification
    if json_content["type"] == "Job":
        result = document.replace_one(
            filter={"id": json_content["id"], "master": json_content["master"]},
            replacement=json_content,
            upsert=True,
        )
    else:
        result = document.insert_one(json_content)

    if result is None:
        logger.debug(f"to_database error for object {type(j)}")
        raise DatabaseError(
            Exception,
            f"Object could not be inserted in the document linked by {document.name}. Insertion resulted in: {result}",
        )
    return result


def object_from_database(_filter, document):
    """Load JobObject from a json filestream

    Parameters
    ----------
    _filter (dict): The key-value pair used to search the object in the document
    document (pymongo.document) : The document of database where the object json will be stored
    """
    content = document.find_one(filter=_filter)
    if content is None:
        logger.debug(
            f"to_database error for `filter` {_filter} and `document` {document.name}"
        )
        raise DatabaseError(
            Exception,
            f"{_filter} pair was not found in the document linked by {document.name}",
        )
    loader = JsonLoader()
    obj, error = loader.parse_static(content)
    return obj, error


def index_to_database(data, document):
    """Save the index information into the `index` document of the database

    Parameters
    ----------
    data (dict) : Index/Cache information of the object in consideration
    document (pymongo.document) : The document of database where the object json will be stored
    """
    if data:
        data["modified_time"] = time.time()
        if "id" in data and "master" in data:
            result = document.replace_one(
                filter={"id": data["id"], "master": data["master"]},
                replacement=data,
                upsert=True,
            )
        else:
            result = document.insert_one(data)

        if result is None:
            raise DatabaseError(
                Exception,
                f"index could not be inserted in the document linked by {document.name}. Insertion resulted in: {result}",
            )

        return result


def index_from_database(_filter, document, many=False):
    """Save the index information into the `index` document of the database

    Parameters
    ----------
    _filter (dict): The key-value pair used to search the object in the document
    document (pymongo.document): The document of database where the object json will be stored
    many (bool): Return all the values that satisfy the filter
    """
    if many:
        result = [*document.find(filter=_filter)]
    else:
        result = document.find_one(filter=_filter)

    if result is None:
        raise DatabaseError(
            Exception,
            f"index could not be extracted in the document linked by {document.name}. Extracted resulted in: {result}:{_filter}",
        )
    return result


# TODO: Remove the versioning
class EmptyGangaObject(GangaObject):
    """Empty Ganga Object. Is used to construct incomplete jobs"""

    _schema = Schema(Version(0, 0), {})
    _name = "EmptyGangaObject"
    _category = "internal"
    _hidden = 1

    def __init__(self):
        super(EmptyGangaObject, self).__init__()


# TODO: This is currently not in use, idea behind use is that
## docker container/database shuts or becomes irresponsive
class DockerIncessableError(GangaException):
    message = "raise this error, when the database timeout passes"
    pass
    # raise NotImplemented


class DatabaseError(GangaException):
    def __init__(self, excpt, message):
        GangaException.__init__(self, excpt, message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            err = "(%s:%s)" % (type(self.excpt), self.excpt)
        else:
            err = ""
        return "DatabaseError: %s %s" % (self.message, err)


class JsonDumper:
    """Used by Ganga Objects to convert themselves into a json representation of their components
    """

    def __init__(self, location=None):
        self.errors = []
        self.location = location

    def parse(self, j, ignore_subs=[]):
        """Will parse and return the job json
        The received item is a job object with proxy
        """
        starting_name, starting_node = "Job", j
        job_json = JsonDumper.object_to_json(
            starting_name, starting_node, ignore_subs)
        return job_json

    @staticmethod
    def object_to_json(name, node, ignore_subs):
        """Will give the attribute information of the provided `node` object as a python dict
        """
        # instead it could be: node._schemae.name not in ignore_subs
        if name not in ignore_subs:
            if not hasattr(node, "_schema"):
                return

            node_info = {
                "type": node._schema.name,
                "version": f"{node._schema.version.major}.{node._schema.version.minor}",
                "category": node._schema.category,
            }

            if node._schema is None:
                return node_info

            for attr_name, attr_object in node._schema.allItems():
                value = getattr(node, attr_name)
                if attr_name == "_list":
                    temp_val = []
                    for val in value:
                        temp_val.append(val)
                    node_info[attr_name] = temp_val

                if attr_object["visitable"]:
                    # if isType(value, list) or isType(value, tuple):
                    #     node_info[attr_name] = acceptOptional(value)
                    if isinstance(value, GangaObject):
                        node_info[attr_name] = JsonDumper.object_to_json(
                            attr_name, value, ignore_subs
                        )
                    elif isinstance(value, dict) and attr_name == "timestamps":
                        for time_stamp, dtime in value.items():
                            if isinstance(dtime, datetime.datetime):
                                value[time_stamp] = dtime.strftime(
                                    "%Y/%m/%d %H:%M:%S")
                            node_info[attr_name] = value
                    else:
                        node_info[attr_name] = value

                return node_info


class JsonLoader:
    """Loads the Ganga Object from json
    """
    @staticmethod
    def parse_static(json_content):
        """This implementation is backwards compatible to the way things are currently in VStreamre
        """
        errors = []
        obj = allPlugins.find(
            json_content["category"], json_content["type"]).getNew()

        # FIXME: Use a better approach to filter the metadata keys
        for key in set(json_content.keys()) - set(["category", "type", "version"]):
            if (
                isinstance(json_content[key], dict)
                or isinstance(json_content[key], list)
            ) and "category" in json_content[key]:
                obj, local_error = JsonLoader.load_component_object(
                    obj, key, json_content[key]
                )
                if local_error:
                    errors.append(local_error)

            else:
                obj, local_error = JsonLoader.load_simple_object(
                    obj, key, json_content[key]
                )
                if local_error:
                    errors.append(local_error)

        return obj, errors

    @staticmethod
    def load_component_object(parent_obj, name, part_attr):
        """Loading component objects that will be attached to the main object
        """
        errors = []
        try:
            component_obj = allPlugins.find(
                part_attr["category"], part_attr["type"]
            ).getNew()
        except PluginManagerError as e:
            # TODO: Maybe move this to the logger
            print(e)
            errors.append(e)
            component_obj = EmptyGangaObject()

        # Assigning the component object its attributes
        for attr, _ in component_obj._schema.allItems():
            if attr in part_attr:
                # loader component attribute fo this component attribute
                if isinstance(part_attr[attr], list):
                    temp_val = []
                    for val in part_attr[attr]:
                        if isinstance(val, dict) and "category" in part_attr[attr]:
                            itr_obj, err = JsonLoader.load_list_object(val)
                            if err:
                                errors.append(err)
                            temp_val.append(itr_obj)
                        else:
                            # itr_obj, err = JsonLoader.load_simple_object(component_obj, attr, val)
                            temp_val.append(val)

                    component_obj, local_error = JsonLoader.load_simple_object(
                        component_obj, attr, temp_val
                    )
                    if local_error:
                        errors.append(local_error)
                elif (
                    isinstance(part_attr[attr],
                               dict) and "category" in part_attr[attr]
                ):
                    component_obj, local_error = JsonLoader.load_component_object(
                        component_obj, attr, part_attr[attr]
                    )
                else:
                    try:
                        component_obj.setSchemaAttribute(attr, part_attr[attr])
                    except Exception as e:
                        errors.append(e)
                        raise GangaException(
                            "ERROR in loading Json, failed to set attribute %s for class %s"
                            % (attr, type(component_obj))
                        )

        # Attach the component object to the master object
        parent_obj.setSchemaAttribute(name, component_obj)
        return parent_obj, errors

    @staticmethod
    def load_list_object(part_attr):
        """Loading component objects that will be attached to a list object
        """
        errors = []
        try:
            component_obj = allPlugins.find(
                part_attr["category"], part_attr["type"]
            ).getNew()
        except PluginManagerError as e:  # TODO: Maybe move this to the logger
            print(e)
            errors.append(e)
            component_obj = EmptyGangaObject()

        # Assigning the component object its attributes
        for attr, _ in component_obj._schema.allItems():
            if attr in part_attr:
                try:
                    component_obj.setSchemaAttribute(attr, part_attr[attr])
                except Exception as e:
                    errors.append(e)
                    raise GangaException(
                        "ERROR in loading Json, failed to set attribute %s for class %s"
                        % (attr, type(component_obj))
                    )

        return component_obj, errors

    @staticmethod
    def load_simple_object(parent_obj, name, value):
        """Attaching a simple attribute to the ganga object
        """
        errors = []
        try:
            parent_obj.setSchemaAttribute(name, value)
        except Exception as e:
            errors.append(e)
            raise GangaException(
                "ERROR in loading Json, failed to set attribute %s for class %s"
                % (name, type(parent_obj))
            )
        return parent_obj, errors

"""
Will be used later on
from GangaCore.Utility.Config import getConfig

    _ = pymongo.MongoClient()
    db_name = "dumbmachine"
    connection = _[db_name]

    prep_metadata_path = os.path.join(getLocalRoot(), '6.0', 'prep.metadata')

    # adding the metadata objects
    for _dir in os.listdir(os.path.join(prep_metadata_path, "0xxx")):
        _dir = os.path.join(prep_metadata_path, "0xxx", _dir)
        if os.path.isdir(_dir):
            data, er = from_file(open(os.path.join(_dir, "data"), "rb"))
            object_to_database(j=data, document=connection["prep.metadata"],
                               master=-1, ignore_subs=[])


def templates_metadata_migrate():
    """Convert the XMl files to Database
    """
    import os
    import time
    import pickle
    import pymongo

    from GangaCore.GPIDev.Base.Proxy import getName, addProxy
    from GangaCore.Runtime.Repository_runtime import getLocalRoot
    from GangaCore.Core.GangaRepository.VStreamer import from_file
    from GangaCore.test.GPI.newXMLTest.utilFunctions import getXMLDir, getXMLFile

    _ = pymongo.MongoClient()
    db_name = "dumbmachine"
    connection = _[db_name]

    templates_metadata_path = os.path.join(
        getLocalRoot(), '6.0', 'templates.metadata')

    # adding the metadata objects
    for _dir in os.listdir(os.path.join(templates_metadata_path, "0xxx")):
        _dir = os.path.join(templates_metadata_path, "0xxx", _dir)
        if os.path.isdir(_dir):
            data, er = from_file(open(os.path.join(_dir, "data"), "rb"))
            object_to_database(j=data, document=connection["templates.metadata"],
                               master=-1, ignore_subs=[])


def get_job_done():
    # tasks_path = os.path.join(getLocalRoot(), '6.0', 'tasks')
    # preps_path = os.path.join(getLocalRoot(), '6.0', 'preps')
    # templates_path = os.path.join(getLocalRoot(), '6.0', 'templates')

    # box_metadata_path = os.path.join(getLocalRoot(), '6.0', 'box.metadata')
    # jobs_metadata_path = os.path.join(getLocalRoot(), '6.0', 'jobs.metadata')
    # prep_metadata_path = os.path.join(getLocalRoot(), '6.0', 'prep.metadata')
    # box_path = os.path.join(getLocalRoot(), '6.0', 'box')

    job_migrate()
    job_metadata_migrate()
    prep_metadata_migrate()


# """
# Will be used later on
# from GangaCore.Utility.Config import getConfig

#     # # getting the options from the config
#     # c = getConfig("DatabaseConfigurations")

#     # if c["database"] == "default":
#     #     path = getConfig("Configuration")["gangadir"]
#     #     conn = "sqlite:///" + path + "/ganga.db"
#     # else:
#     #     raise NotImplementedError("Other databases are not supported")

#     #     import urllib

#     #     dialect = c["database"]
#     #     driver = c["driver"]
#     #     username = urllib.parse.quote_plus(c["username"])
#     #     password = urllib.parse.quote_plus(c["password"])
#     #     host = c["host"]
#     #     port = c["port"]
#     #     database = c["dbname"]

#     # mongouri = f"mongodb://{username}:{password}@{host}:{port}/"
#     # client = pymongo.MongoClient(mongouri)
# """

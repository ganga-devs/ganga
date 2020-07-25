# TODO: Remove unrequired imports
import os
import json
import time
import pymongo
import datetime

from pymongo import ReturnDocument
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.Plugin import PluginManagerError, allPlugins
from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Base.Proxy import addProxy, isType, stripProxy
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList

logger = getLogger()


def object_to_database(j, document, master=None, ignore_subs=[]):
    """Save GangaObject in database

    Arguments:
    j (GangaObject): The job to be converted
    master (int): Index id of the master Job of `j`, if `j` is a subjob
    document : The document of database where the job json will be stored
    """
    logger.debug("object_to_database")
    json_content = j.to_json()
    for sub in ignore_subs:
        json_content.pop(sub, None)

    json_content["modified_time"] = time.time()
    if master is not None:
        json_content["master"] = master

    if json_content["type"] == "Job":
        # `id` is used for indexing by mongo.
        # json_content["id"] = json_content["id"]
        result = document.replace_one(
            filter={"id": json_content["id"], "master": json_content["master"]}, replacement=json_content, upsert=True,
        )
    else:
        result = document.insert_one(json_content)

    if result is None:
        logger.debug(f"to_database error for object {j}")
        raise DatabaseError(
            Exception,
            f"Object could not be inserted in the document linked by {document.name}. Insertion resulted in: {result}",
        )
    return result


def object_from_database(_filter, document):
    """Load JobObject from a json filestream

    Arguments:
    _filter (dict): The key-value pair used to search the object in the document
    document: The document in the database where the object is stored
    """
    logger.debug("object_from_database")
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

    Args:
        data : To be added
        document : To be added
    """
    logger.debug("index_to_database")
    if data:
        data["modified_time"] = time.time()
        if "id" in data and "master" in data:
            result = document.replace_one(
                filter={"id": data["id"], "master": data["master"]}, replacement=data, upsert=True,
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

    Args:
        _filter : To be added
        document : To be added
    """
    logger.debug("index_from_database")
    if many:
        result = [*document.find(filter=_filter)]
    else:
        result = document.find_one(filter=_filter)

    # if result is None:
    #     raise DatabaseError(
    #         Exception,
    #         f"index could not be extracted in the document linked by {document.name}. Extracted resulted in: {result}:{_filter}",
    #     )
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

        def acceptOptional(s):
            """
            """
            if isType(s, str):
                return s
            elif isType(s, GangaList) or isType(s, list) or isType(s, tuple):
                for sub_s in s:
                    sub_val = acceptOptional(sub_s)
            elif hasattr(s, "accept"):
                return self.componentAttribute(None, s, node, ignore_subs)
            else:
                return repr(s)

        def handle_gangalist(glist):
            if hasattr(glist, "accept"):
                for (name, item) in glist._schema.allItems():
                    if name == "_list":
                        values = getattr(glist, name)
                        # ret_values = []
                        for val in values:
                            values.append(handle_gangalist(val))
                    elif item["visitable"]:
                        return
            else:
                return glist

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
                    if isType(value, list) or isType(value, tuple):
                        node_info[attr_name] = acceptOptional(value)
                    elif isinstance(value, GangaObject):
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

    def simpleAttribute(self, attr_name, value, sequence):
        """
        Adding simple attribute's information to the master node
        """
        if sequence:
            for v in value:
                self.optional()
        else:
            # This case means that the value is another component object
            if isinstance(value, GangaObject):
                self.optional()
            else:
                return value

    def componentAttribute(self):
        """
        Adding component attributes's information to the master node
        """
        pass

    def sharedAttribute(self, attr_name, value, sequence):
        """
        Adding a shared attribute's information to the master node
        Uses simpleAttribute's implemenation under the hood
        """
        self.simpleAttribute(attr_name, value, sequence)

    @staticmethod
    def componentAttribute(self, attr_name, value, ignore_subs):
        """
        """
        if isType(value, (list, tuple, GangaList)):
            ret_val = list(value)
        else:
            ret_val = JsonDumper.object_to_json(
                attr_name, getattr(node, attr_name), ignore_subs
            )
        return ret_val


class JsonLoader:
    """Loads the Ganga Object from json
    """

    def __init__(self):
        """Initializing the required variables
        """
        self.errors = []
        self.json_content = None

    def parse(self):
        """Parse and load the oppropriate things
        """
        # Process starts with the creation of the parent object
        self.obj = allPlugins.find(
            self.json_content["category"], self.json_content["type"]
        ).getNew()

        # TODO: Use a better approach to filter the metadata keys
        for key in set(self.json_content.keys()) - set(["category", "type", "version"]):

            # dict implies that the object to be loaded will be a GangaObject object
            if isinstance(self.json_content[key], dict):
                self.obj = self.load_component_object(
                    self.obj, key, self.json_content[key]
                )

            # list implies that we are trying to load either
            # - a list of simples values (we handle it like a list) or
            # - a GangaList list (we will try to load the objects of this list as if they are component objects)
            if isinstance(self.json_content[key], list):
                temp_val = []
                for val in json_content[key]:
                    if isinstance(val, dict):
                        temp_val.append(
                            self.load_component_object(self.obj, key, val))
                    else:
                        temp_val.append(
                            self.load_simple_object(self.obj, key, val))

                # simply attach loaded list of component objects to its parent object
                self.obj = self.load_simple_object(self.obj, key, temp_val)

            else:
                self.obj = self.load_simple_object(
                    self.obj, key, self.json_content[key]
                )

        return self.obj, self.errors

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
        for attr, item in component_obj._schema.allItems():
            if attr in part_attr:
                # loader component attribute fo this component attribute
                if isinstance(part_attr[attr], list):
                    temp_val = []
                    for val in part_attr[attr]:
                        if isinstance(val, dict) and "category" in part_attr[attr]:
                            itr_obj, err = JsonLoader.load_list_object(val)
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

        # Assigning the component object to the master object
        parent_obj.setSchemaAttribute(name, component_obj)
        # I do not think we should return error, as it is already raised before
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
        for attr, item in component_obj._schema.allItems():
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


# class XmlToDatabaseConverter:
#     """This will ensure full backwards compatibilty.
#     Functions for creating LocalJson repo from LocalXML and vice versa.
#     """
#     import os
#     import pickle

#     from GangaCore.GPIDev.Base.Proxy import getName, addProxy
#     from GangaCore.Runtime.Repository_runtime import getLocalRoot
#     from GangaCore.Core.GangaRepository.VStreamer import from_file
#     from GangaCore.Core.GangaRepository.DStreamer import (
#         index_from_database, index_to_database,
#         object_from_database, object_to_database
#     )
#     from GangaCore.test.GPI.newXMLTest.utilFunctions import getXMLDir, getXMLFile

#     import pymongo
#     _ = pymongo.MongoClient()
#     db_name = "dumbmachine"
#     connection = _[db_name]

#     box_path = os.path.join(getLocalRoot(), '6.0', 'box')
#     jobs_path = os.path.join(getLocalRoot(), '6.0', 'jobs')
#     tasks_path = os.path.join(getLocalRoot(), '6.0', 'tasks')
#     preps_path = os.path.join(getLocalRoot(), '6.0', 'preps')
#     templates_path = os.path.join(getLocalRoot(), '6.0', 'templates')

#     box_metadata_path = os.path.join(getLocalRoot(), '6.0', 'box.metadata')
#     jobs_metadata_path = os.path.join(getLocalRoot(), '6.0', 'jobs.metadata')
#     prep_metadata_path = os.path.join(getLocalRoot(), '6.0', 'prep.metadata')

#     job_ids = [i for i in os.listdir(os.path.join(jobs_path, "0xxx"))
#                if "index" not in i]
#     indexes = []
#     for idx in job_ids:
#         job_file = getXMLFile(int(idx))
#         job_folder = os.path.dirname(job_file)
#         jeb, err = from_file(open(job_file, "rb"))
#         _, _, index = pickle.load(
#             open(job_file.replace("/data", ".index"), "rb"))

#         # check for subjobs
#         if "subjobs.idx" in os.listdir(job_folder):
#             # Will store the subjobs information as well
#             subjob_ids = [i for i in os.listdir(job_folder) if i.isdecimal()]
#             subjob_files = [os.path.join(job_folder, i, "data")
#                             for i in subjob_ids]
#             subjob_indexes = pickle.load(
#                 open(os.path.join(job_folder, "subjobs.idx"), "rb"))
#             for s_idx in subjob_indexes:
#                 index = subjob_indexes[s_idx]
#                 index["master"] = jeb.id
#                 index["classname"] = "Job"
#                 index["category"] = "jobs"
#                 index_to_database(data=index, document=connection.index)

#             for file in subjob_files:
#                 s_jeb = from_file(open(file, "rb"))
#                 object_to_database(j=jeb, document=connection.jobs,
#                                    master=jeb.id, ignore_subs=[])

#         index["master"] = -1  # normal object do not have a master/parent
#         index["classname"] = getName(jeb)
#         index["category"] = jeb._category
#         # index["modified_time"] = time.time()
#         indexes.append(index)
#         index_to_database(data=index, document=connection.index)
#         object_to_database(j=jeb, document=connection.jobs,
#                            master=-1, ignore_subs=[])


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

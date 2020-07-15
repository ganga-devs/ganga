# TODO: Remove unrequired imports
import os
import copy
import json
import time
import pymongo
import datetime


from pymongo import ReturnDocument
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.Plugin import PluginManagerError, allPlugins
from GangaCore.GPIDev.Base.Objects import GangaObject, ObjectMetaclass
from GangaCore.GPIDev.Base.Proxy import addProxy, isType, stripProxy
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaList

# TODO: Use the logger often, instead of print [next-commit]
logger = getLogger()

# creating a connection for testing purposes
_ = pymongo.MongoClient()
connection = _.dumbmachine.objects

# ignore_subs was added for backwards compatibility
def to_file(j, fobj=None, ignore_subs=[]):
    """Convert JobObject and write to fileobject
    """
    try:
        json_content = j.to_json()
        for sub in ignore_subs:
            json_content.pop(sub, None)
        if fobj is None:
            print(json_content)
        else:
            json.dump(json_content, fobj)
    except Exception as err:
        logger.error("Json to-file error for file:\n%s" % (err))
        raise DatabaseError(err, "to-file error")


def from_file(f):
    """Load JobObject from a json filestream
    """
    try:
        json_content = json.load(f)
        loader = JsonLoader()
        obj, error = loader.parse_static(json_content)
        return obj, error
    except Exception as err:
        logger.error("Json from-file error for file:\n%s" % err)
        # raise DatabaseError(err, "from-file error")
        raise DatabaseError(err, f"from-file error :: {f}")


# ignore_subs was added for backwards compatibilty
def to_database(j, document, master=None, ignore_subs=[]):
    """Convert JobObject and write to file object

    Arguments:
    j (GangaObject): The job to be converted
    documnent (pymongo): The document of database where the job json will be stored
    master (int): Index id of the master Job of `j`, if `j` is a subjob
    """
    json_content = j.to_json()
    for sub in ignore_subs:
        json_content.pop(sub, None)

    json_content["modified_time"] = time.time()
    # Mongo uses _id as indexing identifier
    # TODO: We dont really need that _id
    logger.info(json_content["type"])
    if json_content["type"] == "Job":
        json_content["_id"] = json_content["id"]

        result = document.find_one_and_update(
            filter={"_id": json_content["_id"]},
            update={"$set": json_content},
            upsert=True,
            projection="name",
            return_document=ReturnDocument.AFTER,  # updation, sometimes (during upsert) does not return any flag confirmation of the update, this forces that
        )
        # result = document.find_one_and_update(
        #     filter={"id": json_content["id"]},
        #     replacement=json_content,
        #     upsert=True,
        #     projection="id"
        # )
        logger.info(f"updated gave this {result}")
    else:
        result = document.insert(json_content)
        logger.info(f"insertion gave this {result}")

    if result is None:
        # log the error
        # logger.error("Database to-file error for file:\n%s" % (err))
        raise DatabaseError(
            Exception,
            f"{j} could not be inserted in the document linked by {document.name}. Inserted resulted in: {result}",
        )
    return result


def from_database(document, attribute, value):
    """Load JobObject from a json filestream

    Will connect to the document indicated by document and then search for the appropriate
    object using the identifier
    """
    content = document.find_one({attribute: value})
    if content is None:
        logger.error("from-database error for database")
        raise DatabaseError(
            Exception,
            f"({attribute}, {value}) pair was not found in the document linked by {document.name}",
        )
    loader = JsonLoader()
    obj, error = loader.parse_static(content)
    return obj, error


# TODO: Remove the versioning from this
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


# Kept for backwards compatibility purposes
class JsonFileError(GangaException):
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
        job_json = JsonDumper.object_to_json(starting_name, starting_node, ignore_subs)
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
                                value[time_stamp] = dtime.strftime("%Y/%m/%d %H:%M:%S")
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

    def __init__(self, location=None):
        """Initializing the required variables
        """
        self.errors = []
        # this information can also be discarded
        self.location = location
        self.json_content = None

    def _read_json(self):
        """Parse the json into python dict
        """
        if os.path.isfile(self.location):
            self.json_content = json.load(open(self.location, "r"))
        else:
            raise FileNotFoundError(f"The file {self.location} could not be found")

    def parse(self):
        """Parse and load the oppropriate things
        """
        # creation process starts with the creation of the JoB
        # Creating the job objects
        if self.json_content is None:
            self._read_json()

        self.obj = allPlugins.find(
            self.json_content["category"], self.json_content["type"]
        ).getNew()

        # FIXME: Use a better approach to filter the metadata keys
        for key in set(self.json_content.keys()) - set(["category", "type", "version"]):

            # dict implies that the object to be loaded will be a component object (or say a GangaObject itself)
            if isinstance(self.json_content[key], dict):
                self.obj = self.load_component_object(
                    self.obj, key, self.json_content[key]
                )

            # list implies that we are trying to load either
            # a list of simples values (we handle it like a list) or
            # a GangaList list (we will try to load the nested objects of this list as if they are component objects)
            if isinstance(self.json_content[key], list):
                temp_val = []
                for val in json_content[key]:
                    if isinstance(val, dict):
                        temp_val.append(self.load_component_object(self.obj, key, val))
                    else:
                        temp_val.append(self.load_simple_object(self.obj, key, val))

                # once the nested objects from the list have been loaded, the list can simply be attached to its parent object
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
        obj = allPlugins.find(json_content["category"], json_content["type"]).getNew()

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
                    isinstance(part_attr[attr], dict) and "category" in part_attr[attr]
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


# TODO: Add more functions here
class XmlToJsonConverter:
    """This will ensure full backwards compatibilty. Functions for creating LocalJson repo from LocalXML and vice versa.
    """

    # FIXME: Better approach for conversion is to read and parse the xml, instead of calling the VStreamer functions
    @staticmethod
    def xml_to_json(fobj, location):
        """Converts the xml job representation to json representation
        """
        from GangaCore.GPIDev.Base.Proxy import stripProxy
        from GangaCore.Core.GangaRepository.JStreamer import to_file as json_to_file
        from GangaCore.Core.GangaRepository.VStreamer import from_file as xml_from_file

        # loading the job from xml rep
        job, error = xml_from_file(fobj)
        stripped_j = stripProxy(job)

        # saving the job as a json now
        with open(location, "r") as fout:
            json_to_file(stripped_j, fobj=fout)

    @staticmethod
    def json_to_xml(fobj, location):
        """Converts the json job representation to xml one
        """
        from GangaCore.GPIDev.Base.Proxy import stripProxy
        from GangaCore.Core.GangaRepository.VStreamer import to_file as xml_to_file
        from GangaCore.Core.GangaRepository.JStreamer import from_file as json_from_file

        # loading the job from json rep
        job, error = json_from_file(fobj)
        stripped_j = stripProxy(job)

        # saving the job as a xml now
        with open(location, "r") as fout:
            xml_to_file(stripped_j, fobj=fout)


"""
Will be used later on
    # # getting the options from the config
    # c = getConfig("DatabaseConfigurations")

    # if c["database"] == "default":
    #     path = getConfig("Configuration")["gangadir"]
    #     conn = "sqlite:///" + path + "/ganga.db"
    # else:
    #     raise NotImplementedError("Other databases are not supported")

    #     import urllib

    #     dialect = c["database"]
    #     driver = c["driver"]
    #     username = urllib.parse.quote_plus(c["username"])
    #     password = urllib.parse.quote_plus(c["password"])
    #     host = c["host"]
    #     port = c["port"]
    #     database = c["dbname"]

    # mongouri = f"mongodb://{username}:{password}@{host}:{port}/"
    # client = pymongo.MongoClient(mongouri)
"""

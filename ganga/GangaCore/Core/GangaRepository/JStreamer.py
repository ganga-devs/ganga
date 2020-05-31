import os
import json
import copy

from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.Plugin import PluginManagerError, allPlugins
from GangaCore.GPIDev.Base.Objects import GangaObject, ObjectMetaclass
from GangaCore.GPIDev.Base.Proxy import addProxy, getName, isType, stripProxy
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaList

logger = getLogger()


class EmptyGangaObject(GangaObject):

    """Empty Ganga Object. Is used to construct incomplete jobs"""
    # TODO: Remove the versioning from this
    _schema = Schema(Version(0, 0), {})
    _name = "EmptyGangaObject"
    _category = "internal"
    _hidden = 1

    def __init__(self):
        super(EmptyGangaObject, self).__init__()


class JsonFileError(GangaException):

    def __init__(self, excpt, message):
        GangaException.__init__(self, excpt, message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            err = '(%s:%s)' % (type(self.excpt), self.excpt)
        else:
            err = ''
        return "JsonFileError: %s %s" % (self.message, err)


def to_file(j, fobj=None):
    """Convert JobObject and write to fileobject
    """
    try:
        json_content = JsonDumper().parse(j)
        json.dump(json_content, fobj)
    except Exception as err:
        logger.error("Json to-file error for file:\n%s" % (err))
        raise JsonFileError(err, "to-file error")



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
        raise JsonFileError(err, "from-file error")        


# TODO: Add error handling and logging for the recursion operations 
class JsonDumper:
    """Will dump the Job in a JSON file
    """

    def __init__(self, location=None):
        self.errors = []
        self.location = location

    def parse(self, j):
        """Will parse and return the job json
        The received item is a job object with proxy
        """
        starting_name, starting_node = "Job", j
        job_json = JsonDumper.object_to_json(starting_name, starting_node)
        return job_json
        
    @staticmethod
    def object_to_json(name, node):
        """Will give the attribute information of the provided `node` object as a python dict
        """
        print(name, node._schema.name)
        node_info = {
            "name": node._schema.name,
            "version": node._schema.version.minor,
            "category": node._schema.category
        }
        for attr_name, attr_object in node._schema.allItems():
            value = getattr(node, attr_name)
            # if attr_name == "time":
                # node_info[attr_name] = JsonDumper.handleDatetime(attr_name, getattr(node, attr_name))
            if isType(value, (list, tuple, GangaList)):
                node_info[attr_name] = list(value)        
            elif isinstance(value, GangaObject):
                node_info[attr_name] = JsonDumper.object_to_json(attr_name, getattr(node, attr_name))
            # ForReview : We could use pickle to save the datetime
            # Reasoning: This method was added to convert the datetime to string before 
            # saving on disk, in the xml implementation it was saved natively as a datetime.datetime object                
            elif isinstance(value, dict) and attr_name == "timestamps":
                for time_stamp, dtime in value.items():
                    value[time_stamp] = dtime.strftime("%Y-%m-%d %H:%M:%S")
                    node_info[attr_name] = value
            else:
                node_info[attr_name] = getattr(node, attr_name)
        return node_info

    # @staticmethod
    # def dump_json(j, location=None):
    #     """Dump the json file
    #     """
    #     job_json = 
    #     if location is None:
    #         print(job_json)
    #     else:
    #         with open(location, "w") as file:
    #             json.dump(job_json, file)

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
            raise FileNotFoundError(
                f"The file {self.location} could not be found")

    def parse(self):
        """Parse and load the oppropriate things
        """
        # creation process starts with the creation of the JoB
        # Creating the job objects
        if self.json_content is None:
            self._read_json()

        self.obj = allPlugins.find(
            self.json_content['category'], self.json_content['name']
        ).getNew()

        # FIXME: Use something better than the below
        for key in (set(self.json_content.keys()) - set(['category', 'name', 'version'])):
            if isinstance(self.json_content[key], dict):
                self.obj = self.load_complex_object(self.obj, key, self.json_content[key])
            else:
                self.obj = self.load_simple_object(self.obj, key, self.json_content[key])

        return self.obj, self.errors

    @staticmethod
    # TODO: 
    def parse_static(json_content):
        """This implementation is backwards compatible to the way things are currently in VStreamre
        """
        # creation process starts with the creation of the JoB
        # Creating the job objects

        obj = allPlugins.find(
            json_content['category'], json_content['name']
        ).getNew()

        # FIXME: Use a better approach to filter the metadata keys
        for key in (set(json_content.keys()) - set(['category', 'name', 'version'])):
            if isinstance(json_content[key], dict):
                obj = JsonLoader.load_complex_object(obj, key, json_content[key])
            else:
                obj = JsonLoader.load_simple_object(obj, key, json_content[key])

        # return obj, errors
        return obj, None


    @staticmethod
    def load_complex_object(master_obj, name, part_attr):
        """Loading Complex objects that will be attached to the main object
        """
        # The "category" field is required by the function and thus is still in use
        # MaybeTODO:
        try:
            component_obj = allPlugins.find(part_attr['category'], part_attr['name']).getNew()
        except PluginManagerError as e:
            print(e)
            component_obj = EmptyGangaObject()

        # Assigning the complex object its attributes
        for attr, item in component_obj._schema.allItems():
            # if value is in `part_attr` assign, else it already has the default value
            if attr in part_attr:
                component_obj.setSchemaAttribute(attr, part_attr[attr])
            else:
                component_obj.setSchemaAttribute(attr, item)

        # Assigning the component object to the master object
        master_obj.setSchemaAttribute(name, component_obj)
        return master_obj

    @staticmethod
    def load_simple_object(master_obj, name, value):
        """Attaching a simple attribute to the ganga object
        """
        try:
            master_obj.setSchemaAttribute(
                name, value
            )
        except:
            raise GangaException(
                "ERROR in loading XML, failed to set attribute %s for class %s" % (name, type(master_obj)))

        return master_obj

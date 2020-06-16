import os
import copy
import json
import datetime

from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.Plugin import PluginManagerError, allPlugins
from GangaCore.GPIDev.Base.Objects import GangaObject, ObjectMetaclass
from GangaCore.GPIDev.Base.Proxy import addProxy, isType, stripProxy
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaList

# debug
from GangaCore.GPIDev.Lib.Registry.PrepRegistry import ShareRef

# TODO: Use the logger often, instead of print [next-commit] 
logger = getLogger()


# ignore_subs was added for backwards compatibilty
def to_file(j, fobj=None, ignore_subs=[]):
    """Convert JobObject and write to fileobject
    """
    try:
        # None implying to print the jobs information
        json_content = JsonDumper().parse(j, ignore_subs=ignore_subs)
        if fobj is None:
            print(json_content)
        else:
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
        # raise JsonFileError(err, "from-file error")        
        raise JsonFileError(err, f"from-file error :: {f}")


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

# skips the subjobs node
class JsonDumper:
    """Will dump the Job in a JSON file
    """

    def __init__(self, location=None):
        self.errors = []
        self.location = location

    def parse(self, j, ignore_subs=[]):
        """Will parse and return the job json
        The received item is a job object with proxy
        """
        # FIXME: assign a proper job.name to ""
        starting_name, starting_node = "", j
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
            elif hasattr(s, 'accept'):
                return JsonDumper.componentAttribute(None, s, node, ignore_subs)
            else:
                return repr(s)


        def handle_gangalist(glist):
            if hasattr(glist, 'accept'):
                for (name, item) in glist._schema.allItems():
                    if name == "_list":
                        values = getattr(glist, name)

                        ret_values = []
                        for val in values:
                            values.append(handle_gangalist(val))
                    elif item['visitable']:
                        return 
            else:
                return glist

        if name not in ignore_subs:
            if not hasattr(node, '_schema'):
                return 
            
            node_info = {
                "type": node._schema.name,
                "version": f"{node._schema.version.major}.{node._schema.version.minor}",
                "category": node._schema.category
            }

            if node._schema is None:
                return node_info
            
            # debug
            print(f"<object_to_json name='{name}'>")
            
            for attr_name, attr_object in node._schema.allItems():
                value = getattr(node, attr_name)
                if attr_name == "_list": 
                    temp_val = []
                    for val in value:
                        temp_val.append(val)
                    node_info[attr_name] = temp_val

                if attr_object['visitable']:
                    # print("\t", attr_name)
                    # if isType(value, (list, tuple, GangaList)):
                    # if isType(value, GangaList) or isType(value, list) or isType(value, tuple):
                    if isType(value, list) or isType(value, tuple):
                        # The GangaList can be a list of objects or list of lists
                        node_info[attr_name] = acceptOptional(value)
                        print("\t", attr_name, type(value))
                        # node_info[attr_name] = list(value)
                    elif isinstance(value, GangaObject):
                        node_info[attr_name] = JsonDumper.object_to_json(attr_name, value, ignore_subs)
                    elif isinstance(value, dict) and attr_name == "timestamps":
                        for time_stamp, dtime in value.items():
                            if isinstance(dtime, datetime.datetime):
                                value[time_stamp] = dtime.strftime("%Y/%m/%d %H:%M:%S")
                            node_info[attr_name] = value
                    else:
                        node_info[attr_name] = value
                
                print("</object_to_json>")
                return node_info
            # for attr_name, attr_object in node._schme
            # for (attr_name, attr_object) in node._schema.simpleItems():
            #     try:
            #         if attr_object['visitable']:
            #             value = getattr(node, attr_name)
            #             node_info[attr_name] = JsonDumper.simpleAttribute(attr_name, value, node, ignore_subs)
            #     except Exception as e:
            #         print(f"[ERROR] {name} - {attr_name} - {attr_object}")

            # for (attr_name, attr_object) in node._schema.sharedItems():
            #     try:
            #         if attr_object['visitable']:
            #             value = getattr(node, attr_name)
            #             node_info[attr_name] = JsonDumper.simpleAttribute(attr_name, value, node, ignore_subs)
            #     except Exception as e:
            #         print(f"[ERROR] {name} - {attr_name} - {attr_object}")

            # for (attr_name, attr_object) in node._schema.componentItems():
            #     try:
            #         value = getattr(node, attr_name)
            #         if attr_object['visitable']:
            #             node_info[attr_name] = JsonDumper.componentAttribute(attr_name, value, node, ignore_subs)
            #     except Exception as e:
            #         print(f"[ERROR] {name} - {attr_name} - {attr_object}", e)




        else:
            print("THIS WAS THE CASE WHEN THERE WAS X IN IGNORE_SUBS")



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

    def optional(self, node):
        """

        """
        if node is None:
            return None
        else:
            if hasattr(node, 'accept'):
                return node.to_json(self)
            elif isType(node, (list, tuple, GangaList)):
                temp_val = []
                for sub_s in node:
                    temp_val.append(self.optional(node))

                print(self.indent(), '</sequence>', file=self.out)
            else:
                self.print_value(s)
        self.level -= 1            

    
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
            ret_val = JsonDumper.object_to_json(attr_name, getattr(node, attr_name), ignore_subs)
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
            self.json_content['category'], self.json_content['type']
        ).getNew()

        # FIXME: Use a better approach to filter the metadata keys
        for key in (set(self.json_content.keys()) - set(['category', 'type', 'version'])):
            if isinstance(self.json_content[key], dict):
                self.obj = self.load_component_object(self.obj, key, self.json_content[key])
            if isinstance(self.json_content[key], list):
                temp_val = []
                for val in json_content[key]:
                    if isinstance(val, dict):
                        temp_val.append(self.load_component_object(self.obj, key, val))
                    else:
                        temp_val.append(self.load_simple_object(self.obj, key, val))
                self.obj = self.load_simple_object(self.obj, key, temp_val)

            else:
                self.obj = self.load_simple_object(self.obj, key, self.json_content[key])

        return self.obj, self.errors

    @staticmethod
    def parse_static(json_content):
        """This implementation is backwards compatible to the way things are currently in VStreamre
        """
        # creation process starts with the creation of the JoB
        # Creating the job objects
        errors = []
        obj = allPlugins.find(
            json_content['category'], json_content['type']
        ).getNew()

        # FIXME: Use a better approach to filter the metadata keys
        for key in (set(json_content.keys()) - set(['category', 'type', 'version'])):
            print(key)
            if isinstance(json_content[key], dict):
                obj, local_error = JsonLoader.load_component_object(obj, key, json_content[key])
                errors.append(local_error)
            if isinstance(json_content[key], list):
                temp_val = []
                for val in json_content[key]:
                    if isinstance(val, dict):
                        itr_obj, err = JsonLoader.load_list_object(val)
                        temp_val.append(itr_obj)
                    else:
                        itr_obj, err = JsonLoader.load_simple_object(obj, key, val)
                        temp_val.append(itr_obj)

                obj, local_error = JsonLoader.load_simple_object(obj, key, temp_val)
                errors.append(local_error)
            else:
                obj, local_error = JsonLoader.load_simple_object(obj, key, json_content[key])
                errors.append(local_error)

        return obj, errors


    @staticmethod
    def load_list_object(part_attr):
        """Loading component objects that will be attached to a list object
        """
        errors = []        
        try:
            component_obj = allPlugins.find(part_attr['category'], part_attr['type']).getNew()
        except PluginManagerError as e:\
            # TODO: Maybe move this to the logger
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
                        "ERROR in loading Json, failed to set attribute %s for class %s" % (attr, type(component_obj)))


        return component_obj, errors



    @staticmethod
    def load_component_object(master_obj, name, part_attr):
        """Loading component objects that will be attached to the main object
        """
        errors = []        
        try:
            component_obj = allPlugins.find(part_attr['category'], part_attr['type']).getNew()
        except PluginManagerError as e:\
            # TODO: Maybe move this to the logger
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
                        "ERROR in loading Json, failed to set attribute %s for class %s" % (attr, type(component_obj)))


        # Assigning the component object to the master object
        master_obj.setSchemaAttribute(name, component_obj)
        return master_obj, errors

    @staticmethod
    def load_simple_object(master_obj, name, value):
        """Attaching a simple attribute to the ganga object
        """
        errors = []
        try:
            master_obj.setSchemaAttribute(name, value)
        except Exception as e:
            errors.append(e)
            raise GangaException(
                "ERROR in loading Json, failed to set attribute %s for class %s" % (name, type(master_obj)))

        return master_obj, errors


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

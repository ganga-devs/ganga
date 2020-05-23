"""
WIP: Replacing the XML files with json equivalent

- JSON Job loader
+ JSON Job dumper
"""


import os
import json

from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy, isType, getName

from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaListByRef

# config_scope is namespace used for evaluating simple objects (e.g. File, datetime, SharedDir)
from GangaCore.Utility.Config import config_scope

from GangaCore.Utility.Plugin import PluginManagerError, allPlugins

from GangaCore.GPIDev.Base.Objects import GangaObject, ObjectMetaclass
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.GPIDev.Lib.GangaList.GangaList import makeGangaList

# from GangaCore.Core.GangaRepository import SchemaVersionError

import copy

logger = getLogger()


class JsonDumper:
    """Will dump the Job in a JSON file
    """
    def __init__(self, dump_location=None):
        self.errors = []
        self.dump_location = dump_location

    def parse(self, item):
        """WIll parse and return the job json

        The received item is a job object with proxy
        """
        starting_name, starting_node = "job", item
        self.job_json = JsonDumper.object_to_json(starting_name, starting_node)
        

    @staticmethod
    def object_to_json(name, node):
        """Will give the attribute information of the provided `node` object as a python dict
        """
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
                    print(time_stamp, dtime)
                    # value[time_stamp] = dtime.strftime("%Y-%m-%d %H:%M:%S")
                node_info[attr_name] = value
            else:
                node_info[attr_name] = getattr(node, attr_name)
        return node_info


    def dump_json(self):
        """Dump the json file
        """
        if self.dump_location is None:
            print(self.job_json)
        else:
            with open(self.dump_location, "w") as file:
                json.dump(self.job_json, file)

class JsonLoader:
    """Loads the Ganga Object from json
    """

    def __init__(self, location):
        """Initializing the required variables
        """
        self.errors = []
        # this information can also be discarded
        self.location = location
        self.ignore_count = 0
        self.json_content = None
        self.primary_object = None  # This will be the job returned

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
        )

        # FIXME: Use something better than the below
        for key in (set(self.json_content.keys()) - set(['category', 'type', 'version'])):
            # if the attribute is complex attribute, which itself is a object
            if isinstance(self.json_content[key], dict):
                print(key)
                self.load_complex_object(key, self.json_content[key])
            else:
                self.load_simple_object(key, self.json_content[key])

        return self.obj, self.errors

    def load_complex_object(self, name, part_attr):
        """Loading Complex objects that will be attached to the main object
        """
        # The "category" field is required by the function and thus is still in use
        # MaybeTODO:
        try:
            obj = allPlugins.find(part_attr['category'], part_attr['type'])
        except PluginManagerError as e:
            print(e)
            self.errors.append(e)
            obj = EmptyGangaObject()
            self.ignore_count = 1
        # else:
        #     version = Version(*[int(v)
        #                         for v in part_attr['version'].split('.')])
        #     if not obj._schema.version.isCompatible(version):
        #         part_attr['currversion'] = '%s.%s' % (
        #             obj._schema.version.major, obj._schema.version.minor)
        #         self.errors.append(SchemaVersionError(
        #             'Incompatible schema of %(name)s, repository is %(version)s currently in use is %(currversion)s' % part_attr))
        #         obj = EmptyGangaObject()
        #         self.ignore_count = 1
        #     else:
        #         # Initialize and cache a c class instance to use as a classs factory
        #         print("I have no ideam when this case will be used")

        # Assigning the complex object its attributes
        for attr, item in obj._schema.allItems():
            # if value is in `part_attr` assign, else it already has the default value
            if attr in part_attr:
                setattr(obj, attr, part_attr[attr])

        # Assigning the complex object to the ganga job
        setattr(self.obj, name, obj)
        # cls = obj.__class__
        # if isinstance(cls, GangaObject):
        #     for attr, item in cls._schema.allItems():
        #         if attr not in obj._data:
        #             if item.getProperties()['getter'] is None:
        #                 try:
        #                     setattr(
        #                         obj, attr, self._schema.getDefaultValue(attr))
        #                     print(
        #                         "Setting the attrivutes now, ", type(obj))
        #                 except:
        #                     raise GangaException(
        #                         "ERROR in loading XML, failed to set default attribute %s for class %s" % (attr, _getName(obj)))
        pass

    def load_simple_object(self, name, value):
        """Attaching a simple attribute to the ganga object

        Arguments:
            attr {attr} -- attr is the thing to be attached to the object
        {"is_prepared": "None"}
        """
        try:
            # self.obj.setSchemaAttribute(name, value)
            setattr(
                self.obj, name, value
            )
        except:
            raise GangaException(
                "ERROR in loading XML, failed to set attribute %s for class %s" % (name, type(self.obj)))
from Ganga.GPIDev.Base import GangaObject
from Ganga.Core.exceptions import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base.Proxy import addProxy
import copy

class MetadataDict(GangaObject):
    '''MetadataDict class

    Class that represents the dictionary of metadata.
    '''
    _schema = Schema(Version(1,0),{
        'data':SimpleItem(defvalue={},doc='dict data',hidden=1, protected=1)
        })
    _name='MetadataDict'
    _category='metadata'
    _exportmethods = ['__getitem__','__str__']


    def __init__(self):
        super(MetadataDict,self).__init__()

    def __str__(self):
        return str(self.data)

    def __getitem__(self, key):
        return addProxy(copy.deepcopy(self.data[key]))

    def __setitem__(self, key, value):
        from Ganga.GPIDev.Lib.Job.Job import Job
        if key in Job._schema.datadict.keys():
            raise GangaAttributeError('\'%s\' is a reserved key name and cannot be used in the metadata'% key)
        if type(key) is not type(''):
            raise GangaAttributeError('Metadata key must be of type \'str\' not %s'%type(key))
        if type(value) is list or type(value) is tuple or type(value) is dict:
            raise GangaAttributeError('Metadata doesn\'t support nesting data structures at the moment, values of type \'list\', \'tuple\' or \'dict\' are forbidden')
            
        self.data[key]=value
        self._setDirty()
        
    def update(self, dict):

        # this way pick up the checking for free
        for key, value in dict.iteritems():
            self.__setitem__(key, value)
#        self.data.update(dict)

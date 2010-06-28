from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class ConfigSection(GangaObject):
    name=''
    attributes={}

    _schema =  Schema(Version(0,0), {})
    _hidden = 1
    
    def __init__(self,name):
        self.name = name
        self.attributes={}
        
    def addattribute(self, p , val):
        self.attributes[p] = val
        
    def write(self,filename):
        tag = '['+self.name+']\n'
        filename.write(tag)
        filename.write('\n')
        for k, v in self.attributes.items():
            p = '%s = %s\n' % (k, v)
            filename.write(p)
        filename.write('\n')
    

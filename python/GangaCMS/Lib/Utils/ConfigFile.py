from ConfigSection import *

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class ConfigFile(GangaObject):

    sections = []
    outfile  = None
    
    _CRAB_SECTIONS = ['CRAB','CMSSW','USER','GRID']    

    _schema =  Schema(Version(0,0), {})
    _hidden = 1

    def __init__(self,filename):

        for section in self._CRAB_SECTIONS:
            sec = self.getsection(section)
            if sec == None:
                self.sections.append(ConfigSection(section))
  
        self.outfile  = open(filename,'w')

    def append(self,section,command):
        
        sec = self.getsection(section)
        if sec == None:
            print "Error"
        splcmd = command.split('=')
        att = splcmd[0]
        if len ( splcmd ) > 2:
            tmp = splcmd[1:len(splcmd)]
            val = '='.join(tmp)
        else:
            val = splcmd[1]
        sec.addattribute( att , val ) 
        
    def getsection(self,section):
        for sc in self.sections:
            if section == sc.name:
                return sc
        return None

    def write(self):
 
        for section in self.sections:
            section.write( self.outfile )

        self.outfile.close()



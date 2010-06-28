#
# Customized parser for files crab.cfg
# Returns dictionary with ConfParams objects
#
# 08/06/10 @ ubeda
#

from GangaCMS_2.Lib.ConfParams import *
from GangaCMS_2.Lib.Utils import ParserError

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

import os.path


class ConfigFileParser(GangaObject):

    _filename = None
    _schema =  Schema(Version(0,0), {})
    _hidden = 1
    _SECTIONS = None
    
    def __init__(self,filename=None):
        self._SECTIONS = {'CMSSW':CMSSW(),'CRAB':CRAB(),'GRID':GRID(),'USER':USER()}
        self._filename = filename

    def getFilename(self):
        return self._filename

    def getSchema(self):
        return self._SECTIONS

    def parse(self):

        filename = self.getFilename()

        if not filename[-4:] == '.cfg':
            raise ParserError('File "%s" has wrong extension (not .cfg).'%(filename))
        if not os.path.isfile(filename):
            raise ParserError('File "%s" not found.'%(filename))
        
        try:
            file = open(filename,'r')
        except:
            raise ParserError('Could not open file "%s".'%(filename))      

        secContainer = None
        for line in file:
 
            if line[0] != '#':
                line = line.replace('\n','')            
                if len(line):

                    if line[0] == '[' and line[-1] == ']':
                        if not line[1:-1] in self._SECTIONS.keys():
                            raise ParserError('Section "%s" is not a valid section.'%(line[1:-1]))
                        if secContainer != None:
                            self._SECTIONS[secContainer.__class__.__name__] = secContainer
                        secContainer = self._SECTIONS[line[1:-1]]
                
                    else:
                        if secContainer == None :
                            raise ParserError('Parsing without section container.')
                        try:
                            (key,value) = line.replace(' ','').split('=')
                        except ValueError:
                            raise ParserError('Line "%s" badly formatted.'%(line))
                        if key in secContainer.schemadic.keys():
                            secContainer.schemadic[key] = value
                        else: 
                            raise ParserError('Unknown attribute "%s" for section "%s".'%(key,secContainer.__class__.__name__))                    

        self._SECTIONS[secContainer.__class__.__name__] = secContainer

        return self._SECTIONS


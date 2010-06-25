#
# Customized parser for files crab.cfg
# Returns dictionary with ConfParams objects
#
# 08/06/10 @ ubeda
#

from GangaCMS.Lib.ConfParams import *
from GangaCMS.Lib.Utils import ParserError

import os.path

class ConfigFileParser:

    filename = None
    
    def __init__(self,filename):
         self.filename = filename

    def getCRABSections(self):
        return {'CMSSW':CMSSW(),'CRAB':CRAB(),'GRID':GRID(),'USER':USER()}

    def parse(self):
        if not self.filename[-4:] == '.cfg':
            raise ParserError('File "%s" has wrong extension (not .cfg).'%(self.filename))
        if not os.path.isfile(self.filename):
            raise ParserError('File "%s" not found.'%(self.filename))
        
        try:
            file = open(self.filename,'r')
        except:
            raise ParserError('Could not open file "%s".'%(filename))

        SECTIONS = self.getCRABsections()

        secContainer = None
        for line in file:
 
            if line[0] != '#':
                line = line.replace('\n','')            
                if len(line):

                    if line[0] == '[' and line[-1] == ']':
                        if not line[1:-1] in SECTIONS.keys(): 
                            raise ParserError('Section "%s" is not a valid section.'%(line[1:-1]))
                        if secContainer != None:
                            SECTIONS[secContainer.__class__.__name__] = secContainer 
                        secContainer = SECTIONS[line[1:-1]]
                
                    else:
                        if secContainer == None :
                            raise ParserError('Parsing without section container.')
                        try:
                            (key,value) = line.replace(' ','').split('=')
                        except ValueError:
                            raise ParserError('Line "%s" badly formatted.'%(line))
                        if key in secContainer.schemadic:
                            secContainer.schemadic[key] = value
                        else: 
                            raise ParserError('Unknown attribute "%s" for section "%s".'%(key,secContainer.__class__.__name__))                    

        SECTIONS[secContainer.__class__.__name__] = secContainer
        return SECTIONS

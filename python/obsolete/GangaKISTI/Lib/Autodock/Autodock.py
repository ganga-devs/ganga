################################################################################
# SNU DCS Lab. & KISTI Project. 
#
# Autodock.py 
################################################################################

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Lib.File import File
import os

class Autodock(IApplication):
    
    _schema = Schema(Version(2,0), {
        'exe' : SimpleItem(defvalue='/bin/sh',typelist=['str','Ganga.GPIDev.Lib.File.File.File'],doc='A path (string) or a File object specifying an autodock script.'), 
        'args' : SimpleItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File'],sequence=1,strict_sequence=0,doc="List of arguments for the autodock script. Arguments may be strings or File objects."),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'script' : SimpleItem(defvalue='/home/horn/ganga_autodock/autodock.sh',doc="Autodock Script"),
        'protein' : SimpleItem(defvalue='/home/horn/ganga_autodock/1u2y.tar.gz',doc='Protein Name'),
        'ligand' : SimpleItem(defvalue='/home/horn/ganga_autodock/ligands/9004736_1.pdbq',doc='Ligand Name'),
        'parameter' : SimpleItem(defvalue='/home/horn/ganga_autodock/dpf3gen.awk',doc='Docking Parameter File name'),
        'binary' : SimpleItem(defvalue='/home/horn/ganga_autodock/autodock.tar.gz',doc='Autodock Binary')
        } )
    _category = 'applications'
    _name = 'Autodock'
    
    """
    _GUIPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                  { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                          { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]
    """

    def __init__(self):
        super(Autodock,self).__init__()
    
    def configure(self,masterappconfig):
        from Ganga.Core import ApplicationConfigurationError
        import os.path
        
        # do the validation of input attributes, with additional checks for exe property

        def validate_argument(x,exe=None):
            if type(x) is type(''):
                if exe:
                    if not x:
                        raise ApplicationConfigurationError(None,'exe not specified')
                        
                    if len(x.split())>1:
                        raise ApplicationConfigurationError(None,'exe "%s" contains white spaces'%x)

                    dirn,filen = os.path.split(x)
                    if not filen:
                        raise ApplicationConfigurationError(None,'exe "%s" is a directory'%x)
                    if dirn and not os.path.isabs(dirn):
                        raise ApplicationConfigurationError(None,'exe "%s" is a relative path'%x)


            else:
              try:
                  if not x.exists():
                      raise ApplicationConfigurationError(None,'%s: file not found'%x.name)
              except AttributeError:
                  raise ApplicationConfigurationError(None,'%s (%s): unsupported type, must be a string or File'%(str(x),str(type(x))))

        
        validate_argument(self.exe,exe=1)

   
        self.args=[os.path.basename(self.script), os.path.basename(self.protein), os.path.basename(self.ligand), os.path.basename(self.parameter)]
        
        for a in self.args:
            validate_argument(a)
        

        job = self.getJobObject()
        
        # add required files to input sandbox
        job.inputsandbox.append(self.binary)
        job.inputsandbox.append(self.ligand)
        job.inputsandbox.append(self.script)
        job.inputsandbox.append(self.protein)
        job.inputsandbox.append(self.parameter)

        # get ligand name and protein name from file name       
        ligandName = os.path.basename(self.ligand).split(".")[0]
        proteinName = os.path.basename(self.protein).split(".")[0]
        
        # add output file (.dlg) to output sandbox
        job.outputsandbox.append(ligandName+"_"+proteinName+".dlg")

        return (None,None)

config = getConfig('defaults_Autodock') #_Properties
config.options['exe'].type = type(None)

class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

        return c

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.LCG import LCGJobConfig
        
        return LCGJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('Autodock','Local', RTHandler)
allHandlers.add('Autodock','LCG', LCGRTHandler)
allHandlers.add('Autodock','gLite', gLiteRTHandler)

#Add handlers for submitting AutoDock application to Gridway backend and InterGrid backend 
allHandlers.add('Autodock','Gridway',RTHandler)
allHandlers.add('Autodock','InterGrid',LCGRTHandler)

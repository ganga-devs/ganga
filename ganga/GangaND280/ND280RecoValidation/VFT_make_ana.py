################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 16/12/2013
################################################################################

from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import *

from GangaCore.Utility.Config import getConfig

from GangaCore.GPIDev.Lib.File import *
from GangaCore.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.Core.exceptions import ApplicationConfigurationError

import os, shutil, re, time
from GangaCore.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class VFT_make_ana(IPrepareApp):
    """
    VFT_make_ana application running macros/grtf_VFT/make_ana.py

    TODO documentation:
    - It is possible to run make_ana alone or make_ana+make_pdf
    - Explain that the make_ana and make_pdf arguments become respectively
      ana_xxxx and pdf_xxxx in this application's setup arguments
    
    """
    _schema = Schema(Version(1,1), {
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'tree' : SimpleItem(defvalue=None,doc='Name of the TTree to analyze in the input file(s). The default name is ND280ValidTree.', typelist=['str','type(None)']),
        'ana_custom' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'ana_output' : SimpleItem(defvalue=None,doc='Output filename or filenames. Takes a string or a list of strings.', typelist=['str','type(None)']),
        'ana_useropt' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,strict_sequence=0,doc="List of user options to pass to make_ana.py using -0. No need to provide '-O'."),

        'run_pdf' : SimpleItem(defvalue=True,doc='Switch to run make_pdf.py on the make_ana.py output.', typelist=['bool']),
        'pdf_custom' : SimpleItem(defvalue=None,doc='Name of the make_pdf.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_title' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_rdp' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_mcp' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_oldrdp' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_oldmcp' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_rdptitle' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_mcptitle' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_oldrdptitle' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_oldmcptitle' : SimpleItem(defvalue=None,doc='Name of the make_ana.py module to use for this analysis.', typelist=['str','type(None)']),
        'pdf_output' : SimpleItem(defvalue=None,doc='Output filename or filenames. Takes a string or a list of strings.', typelist=['str','type(None)']),
        'pdf_options' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,strict_sequence=0,doc="List of user options to pass to make_pdf.py."),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'VFT_make_ana'
    _scriptname = None
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                  { 'attribute' : 'tree', 'widget' : 'String' },
                  { 'attribute' : 'ana_custom', 'widget' : 'String' },
                  { 'attribute' : 'ana_output', 'widget' : 'String' },
                  { 'attribute' : 'ana_useropt', 'widget' : 'String_List' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ 
                          { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                          { 'attribute' : 'tree', 'widget' : 'String' },
                          { 'attribute' : 'ana_custom', 'widget' : 'String' },
                          { 'attribute' : 'ana_output', 'widget' : 'String' },
                          { 'attribute' : 'ana_useropt', 'widget' : 'String_List' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]


    def __init__(self):
        super(VFT_make_ana,self).__init__()


    def configure(self,masterappconfig):
        
        self.ana_useropt = convertIntToStringArgs(self.ana_useropt)
        args = []
        args.append('${ND280ANALYSISTOOLSROOT:-${RECONUTILSROOT}}/macros/grtf_VFT/make_ana.py')

        job = self.getJobObject()

        if self.cmtsetup is None:
          raise ApplicationConfigurationError('No cmt setup script given.')

        if not self.tree is None:
          args.append('-t')
          args.append(self.tree)

        if not self.ana_custom is None:
          args.append('-c')
          args.append(self.ana_custom)

        if not self.ana_useropt is None:
          for UsrOpt in self.ana_useropt:
            args.append('-O')
            args.append(UsrOpt)

        if self.ana_output is None:
          raise ApplicationConfigurationError('No output file given. Fill the ana_output variable.')
        else:
          args.append('-o')
          args.append(self.ana_output)


        # So get the list of filenames get_dataset_filenames() and create a file containing the list of files and put it in the sandbox
        if job.inputdata is None:
          raise ApplicationConfigurationError('The inputdata variable is not defined.')
        fileList = job.inputdata.get_dataset_filenames()
        if len(fileList) < 1:
          raise ApplicationConfigurationError('No input data file given.')
        args.extend(fileList)

        if self.run_pdf:
          args.append('2>&1 && echo "... GRTF ..." &&')
          args.append('${ND280ANALYSISTOOLSROOT:-${RECONUTILSROOT}}/macros/grtf/pdfgen/make_pdf.py')

          if not 'ana_output' in [self.pdf_rdp, self.pdf_mcp, self.pdf_oldrdp, self.pdf_oldmcp]:
            raise ApplicationConfigurationError('None of the pdf inputs is set to use the make_ana.py output. Please set "pdf_rdp", "pdf_mcp", "pdf_oldrdp", or "pdf_oldmcp" to the value "ana_output"')

          for key in ['pdf_rdp', 'pdf_mcp', 'pdf_oldrdp', 'pdf_oldmcp']:
            if getattr(self, key) == 'ana_output':
              setattr(self, key, self.ana_output)
          

          argDict = { '--custom': 'pdf_custom', '--title': 'pdf_title', '--rdp': 'pdf_rdp', '--mcp': 'pdf_mcp', '--oldrdp': 'pdf_oldrdp', '--oldmcp': 'pdf_oldmcp', '--rdptitle': 'pdf_rdptitle', '--mcptitle': 'pdf_mcptitle', '--oldrdptitle': 'pdf_oldrdptitle', '--oldmcptitle': 'pdf_oldmcptitle', '--out': 'pdf_output' }
#          argDict = { '--custom': self.pdf_custom, '--title': self.pdf_title, '--rdp': self.pdf_rdp, '--mcp': self.pdf_mcp, '--oldrdp': self.pdf_oldrdp, '--oldmcp': self.pdf_oldmcp, '--rdptitle': self.pdf_rdptitle, '--mcptitle': self.pdf_mcptitle, '--oldrdptitle': self.pdf_oldrdptitle, '--oldmcptitle': self.pdf_oldmcptitle, '--out': self.pdf_output }

          for key in argDict:
            if not getattr(self, argDict[key]) is None:
              args.append(key+'='+getattr(self, argDict[key]))

          for opt in self.pdf_options:
            for key in argDict:
              if opt.find(key) > -1 and not getattr(self, argDict[key]) is None:
                raise ApplicationConfigurationError('The make_pdf.py command line argument %s was set through both the ganga application variable "%s" and pdf_options "%s". Use only one of them.' % (key, argDict[key], opt))
            args.append(opt)


        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        script += 'source '+self.cmtsetup+'\n'
        script += 'echo "GRTF ..."\n'
        script += ' '.join(args)+' 2>&1\n'
        script += 'echo "... GRTF"\n'

        from GangaCore.GPIDev.Lib.File import FileBuffer

        scriptname = 'make_ana.sh'
        job.getInputWorkspace().writefile(FileBuffer(scriptname,script),executable=1)

        self._scriptname = job.inputdir+scriptname

        return (None,None)



config = getConfig('defaults_VFT_make_ana') #_Properties
# config.options['exe'].type = type(None)


def convertIntToStringArgs(args):

    result = []
    
    for arg in args:
        if isinstance(arg,int):
            result.append(str(arg))
        else:
            result.append(arg)

    return result



class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        return StandardJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)
        

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.Lib.LCG import LCGJobConfig

        return LCGJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('VFT_make_ana','LSF', RTHandler)
allHandlers.add('VFT_make_ana','Local', RTHandler)
allHandlers.add('VFT_make_ana','PBS', RTHandler)
allHandlers.add('VFT_make_ana','SGE', RTHandler)
allHandlers.add('VFT_make_ana','Slurm', RTHandler)
allHandlers.add('VFT_make_ana','Condor', RTHandler)
allHandlers.add('VFT_make_ana','LCG', LCGRTHandler)
allHandlers.add('VFT_make_ana','gLite', gLiteRTHandler)
allHandlers.add('VFT_make_ana','TestSubmitter', RTHandler)
allHandlers.add('VFT_make_ana','Interactive', RTHandler)
allHandlers.add('VFT_make_ana','Batch', RTHandler)
allHandlers.add('VFT_make_ana','Cronus', RTHandler)
allHandlers.add('VFT_make_ana','Remote', LCGRTHandler)
allHandlers.add('VFT_make_ana','CREAM', LCGRTHandler)


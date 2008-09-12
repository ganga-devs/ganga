from Ganga.GPIDev.Adapters.IMerger import MergerError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version

from Ganga.GPIDev.Lib.File import  File
from Ganga.Lib.Mergers.Merger import AbstractMerger, IMergeTool

from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger, log_user_exception
import commands
import inspect
import os
import string
import subprocess
import tempfile

logger = getLogger()

class _DSTMergeTool(IMergeTool):
    
    _category = 'merge_tools'
    _hidden = 1
    _name = '_DSTMergeTool'
    _schema = IMergeTool._schema.inherit_copy()
    _schema.datadict['merge_opts'] = FileItem(defvalue = None, doc='Path to a options file to use when merging.')
    _schema.datadict['version'] = SimpleItem(defvalue = '', doc='The version of DaVinci to use when merging. (e.g. v19r14)')

    def mergefiles(self, file_list, output_file):
        
        #if no opts file is specified, then use version from instellation
        if self.merge_opts is None or not self.merge_opts.name:
            self.merge_opts = File(os.path.join(os.path.dirname(inspect.getsourcefile(_DSTMergeTool)),'DSTMerger.opts'))
    
        if not os.path.exists(self.merge_opts.name):
            raise MergerError("The options file '%s' needed for merging does not exist." % self.merge_opts.name)
        logger.info("Using the options file '%s'.", self.merge_opts.name)
        
        #this is the bit specifing the files
        output_opts = """
// the output file from the merge
InputCopyStream.Output = "DATAFILE='PFN:%s' TYP='POOL_ROOTTREE' OPT='REC'";

//the files to merge
EventSelector.Input = {""" % output_file
        
        file_sep = ','
        file_len = len(file_list)
        
        for i in xrange(file_len):
            file_name = file_list[i]
            if i == (file_len - 1):
                file_sep = '' #i.e. last entry does not have a comma
            output_opts += """
"DATAFILE='PFN:%s' TYP='POOL_ROOTTREE' OPT='READ'"%s""" % (file_name, file_sep)
        output_opts += """
};"""
        #print output_opts
        
        #write this out to a file
        opts_file_name = tempfile.mktemp('.opts')
        try:
            opts_file = file(opts_file_name,'w')
            opts_file.write(output_opts)
        finally:
            opts_file.close()
            
        if not os.path.exists(opts_file_name):
            raise MergerError("Failed to write tempory options file '%s' during merge" % opts_file_name)
        
        #now run gaudirun via a script
        shell_script = """#!/bin/sh
        
if [ -f ${LHCBHOME}/scripts/SetupProject.sh ]; then
  . ${LHCBHOME}/scripts/SetupProject.sh  --ignore-missing DaVinci %s
else
  echo "Could not find the SetupProject.sh script. Your job will probably fail"
fi
gaudirun.py %s %s
exit $?
""" % (self.version, self.merge_opts.name, opts_file_name)

        #print shell_script

        script_file_name = tempfile.mktemp('.sh')
        try:
            script_file = file(script_file_name,'w')
            script_file.write(shell_script)
        finally:
            script_file.close()
        
        return_code = subprocess.call(['/bin/sh',script_file_name])
        if return_code != 0:
            logger.warning('The DSTMerger returned %i when calling gaudirun' % return_code)
            
        #finally clean up
        os.unlink(script_file_name)
        os.unlink(opts_file_name)
        
        if not os.path.exists(output_file):
            raise MergerError("The output file '%s' was not created" % output_file)


class DSTMerger(AbstractMerger):
    """A merger object for LHCb DST files

    The merger uses DaVinci to combine DST files that have
    been returned *locally* in a job's outputsandbox. As such
    it is mainly useful for microDST files.
    
    The usage is as with other merger objects. See the help for 
    TextMerger or RootMerger for more details.
    
    Example:
    
    dm = DSTMerger()
    dm.files = ['dv.dst']
    
    This object can be attached to a job object or 
    used to merge a list of jobs with its merge 
    method.
    
    It is possible to overide the default opts file
    for performing the merge. A new opts file can 
    be provided via the 'merge_opts' field. This should
    be done with care, as some opts are assumed when
    writing the files for output.
    
    Comments and help requests should be sent to:
    
    lhcb-distributed-analysis@cern.ch
    """
    
    _category = 'mergers'
    _exportmethods = ['merge']
    _name = 'DSTMerger'
    _schema = AbstractMerger._schema.inherit_copy()
    _schema.datadict['merge_opts'] = FileItem(defvalue = None, doc='Path to a options file to use when merging.')
    _schema.datadict['version'] = SimpleItem(defvalue = '', doc='The version of DaVinci to use when merging. (e.g. v19r14)')

    def __init__(self):
        super(DSTMerger,self).__init__(_DSTMergeTool())

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        self.merge_tool.merge_opts = self.merge_opts
        self.merge_tool.version = self.version
        #needed as exportmethods doesn't seem to cope with inheritance
        return super(DTSMerger,self).merge(jobs, outputdir, ignorefailed, overwrite)
    
allPlugins.add(_DSTMergeTool,'merge_tools','_DSTMergeTool')

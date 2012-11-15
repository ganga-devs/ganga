#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""Merges DST files."""
import commands
import inspect
import os
import string
import subprocess
import tempfile
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Adapters.IMerger import IMerger
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import GPIProxyObject
from Ganga.GPIDev.Schema import ComponentItem, FileItem, Schema
from Ganga.GPIDev.Schema import SimpleItem, Version
from Ganga.GPIDev.Lib.File import  File
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.Plugin import allPlugins
from Ganga.Utility.logging import getLogger, log_user_exception
import CMTVersion

logger = getLogger()



#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DSTMerger(IMerger):
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
    _schema = IMerger._schema.inherit_copy()
    docstr = 'Path to a options file to use when merging.'
    _schema.datadict['merge_opts'] = FileItem(defvalue=None, doc=docstr)
    docstr = 'The version of DaVinci to use when merging. (e.g. v19r14)'
    _schema.datadict['version'] = SimpleItem(defvalue='', doc=docstr)


    def selectOptionsFile(self, version_string):
        """Trys to find the correct version of the optsions file to use based on the version."""
        
        dir = os.path.dirname(inspect.getsourcefile(_DSTMergeTool))
        options_dir = os.path.join(dir,'options')
            
        #search for the version of the merge opts which most closly matches 'version'
        import glob
        files = glob.glob(options_dir + os.path.sep + 'DSTMerger*.opts')

        #try to find the best options file to use
        opts_files = {}
        for f in files:
            file_name = os.path.basename(f)
            v = None
            #remove the .opts part
            if file_name.endswith('.opts'):
                file_name = file_name[0:-5]
            #remove the DSTMerger bit
            if file_name.startswith('DSTMerger-'):
                file_name = file_name[10:]
            if file_name:
                v = CMTVersion(file_name)
            else:
                v = CMTVersion()
            opts_files[v] = f
        
        #the result to return
        opts_file = None
        
        #itterate over the versions in order
        keys = opts_files.keys()
        keys.sort()
        saved = keys[-1]#default is latest one
        if version_string:
            version = CMTVersion(version_string)
            for k in keys:
                if version < k:
                    break
                else:
                    saved = k
        opts_file = opts_files[saved]
        return opts_file

    def merge(self, file_list, output_file):
        
        #if no opts file is specified, then use version from installation
        if self.merge_opts is None or not self.merge_opts.name:
            self.merge_opts = File(self.selectOptionsFile(self.version))       
                            
        if not os.path.exists(self.merge_opts.name):
            msg = "The options file '%s' needed for merging does not exist." 
            raise MergerError(msg % self.merge_opts.name)
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
        
        #write this out to a file
        opts_file_name = tempfile.mktemp('.opts')
        try:
            opts_file = file(opts_file_name,'w')
            opts_file.write(output_opts)
        finally:
            opts_file.close()
            
        if not os.path.exists(opts_file_name):
            msg = "Failed to write tempory options file '%s' during merge"
            raise MergerError(msg % opts_file_name)
        
        #now run gaudirun via a script
        shell_script = """#!/bin/sh
        
SP=`which SetupProject.sh`
if [ -n $SP ]; then 
  . SetupProject.sh  --ignore-missing DaVinci %s
else
  echo "Could not find the SetupProject.sh script. Your job will probably fail"
fi
gaudirun.py %s %s
exit $?
""" % (self.version, self.merge_opts.name, opts_file_name)

        script_file_name = tempfile.mktemp('.sh')
        try:
            script_file = file(script_file_name,'w')
            script_file.write(shell_script)
        finally:
            script_file.close()
        
        return_code = subprocess.call(['/bin/sh',script_file_name])
        if return_code != 0:
            msg = 'The DSTMerger returned %i when calling gaudirun'
            logger.warning(msg % return_code)
            
        #finally clean up
        os.unlink(script_file_name)
        os.unlink(opts_file_name)
        
        if not os.path.exists(output_file):
            msg = "The output file '%s' was not created"
            raise MergerError(msg % output_file)
        #needed as exportmethods doesn't seem to cope with inheritance

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Add it to the list of plug-ins
#allPlugins.add(_DSTMergeTool,'merge_tools','_DSTMergeTool')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

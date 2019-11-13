#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""Merges DST files."""
import subprocess
import inspect
import os
import string
import subprocess
import tempfile
from GangaCore.GPIDev.Adapters.IPostProcessor import PostProcessException
from GangaCore.GPIDev.Adapters.IMerger import IMerger
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Base.Proxy import GPIProxyObject
from GangaCore.GPIDev.Schema import ComponentItem, FileItem, Schema
from GangaCore.GPIDev.Schema import SimpleItem, Version
from GangaCore.GPIDev.Lib.File import File
from GangaCore.Utility.Config import makeConfig, ConfigError, getConfig
from GangaCore.Utility.Plugin import allPlugins
from GangaCore.Utility.logging import getLogger, log_user_exception

logger = getLogger()


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbFileMerger(IMerger):

    """A merger object for LHCb files

    The merger uses DaVinci to combine DST files that have
    been returned *locally* in a job's outputsandbox. As such
    it is mainly useful for microDST files.

    The usage is as with other merger objects. See the help for 
    TextMerger or RootMerger for more details.

    Example:

    lhcbm = LHCbFileMerger()
    lhcbm.files = ['dv.dst']

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

    _category = 'postprocessor'
    _exportmethods = ['merge']
    _name = 'LHCbFileMerger'
    _schema = IMerger._schema.inherit_copy()
    docstr = 'The version of DaVinci to use when merging. (e.g. v33r0p1)'
    _schema.datadict['version'] = SimpleItem(defvalue='', doc=docstr)

    def mergefiles(self, file_list, output_file):

        # if no opts file is specified, then use version from installation

        # this is the bit specifing the files
        output_opts = """
outputfile = '%s' """ % output_file
        output_opts += "\ninput = ["
        file_sep = ','
        for f in file_list:
            if f is file_list[-1]:
                file_sep = ''
            output_opts += "'%s' %s " % (f, file_sep)
        output_opts += "]"

        output_opts += """
from GaudiConf import IOHelper

IOHelper().inputFiles(input)
IOHelper().outStream(outputfile,"InputCopyStream")

from Configurables import LHCbApp

LHCbApp().EvtMax = -1
        """

        # write this out to a file
        opts_file_name = tempfile.mktemp('.py')
        #opts_file = open(opts_file_name, 'w')
        try:
            opts_file = open(opts_file_name, 'w')
            opts_file.write(output_opts)
        finally:
            opts_file.close()

        if not os.path.exists(opts_file_name):
            msg = "Failed to write temporary options file '%s' during merge"
            raise PostProcessException(msg % opts_file_name)

        import EnvironFunctions
        script_file_name = EnvironFunctions.construct_merge_script(self.version,
                                                                   opts_file_name)

        return_code = subprocess.call(['/bin/sh', script_file_name])
        if return_code != 0:
            msg = 'The LHCbFileMerger returned %i when calling gaudirun'
            logger.warning(msg % return_code)

        # finally clean up
        os.unlink(script_file_name)
        os.unlink(opts_file_name)

        if not os.path.exists(output_file):
            msg = "The output file '%s' was not created"
            raise PostProcessException(msg % output_file)
        # needed as exportmethods doesn't seem to cope with inheritance

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Add it to the list of plug-ins
allPlugins.add(LHCbFileMerger, 'postprocessor', 'LHCbFileMerger')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Uses gaudirun.py to parse the job options file to allow for easy extraction
of inputdata, outputdata and output files.'''

import tempfile
import fnmatch
import re
from GangaCore.GPIDev.Base.Proxy import stripProxy, getName
from GangaCore.GPIDev.Lib.File import FileBuffer
from GangaCore.GPIDev.Lib.File import LocalFile
import GangaCore.Utility.logging
from GangaCore.Utility.util import unique
import GangaCore.Utility.Config
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from GangaCore.Core.exceptions import ApplicationConfigurationError, GangaTypeError
from GangaCore.Utility.files import expandfilename
from GangaGaudi.Lib.Applications.GaudiUtils import shellEnv_cmd
from GangaCore.GPIDev.Lib.File.OutputFileManager import outputFilePostProcessingOnWN
logger = GangaCore.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

## Due to a bug in Gaudi at some point we need this equivalenc here: see #204
DataObjectDescriptorCollection = str

class PythonOptionsParser(object):


    """ Parses job options file(s) w/ gaudirun.py to extract user's files"""

    def __init__(self, optsfiles, extraopts, env):
        self.optsfiles = optsfiles
        self.extraopts = extraopts
        self.env = env
        self.opts_dict, self.opts_pkl_str = self._get_opts_dict_and_pkl_string()

    def _get_opts_dict_and_pkl_string(self):
        '''Parse the options using gaudirun.py and create a dictionary of the
        configuration and pickle the options. The app handler will make a copy
        of the .pkl file for each job.'''
        tmp_pkl = tempfile.NamedTemporaryFile(suffix='.pkl', mode="rb+")
        tmp_py = tempfile.NamedTemporaryFile(suffix='.py', mode="r+")
        py_opts = tempfile.NamedTemporaryFile(suffix='.py', mode="w")
        py_opts.write(self._join_opts_files())
        py_opts.flush()

        gaudirun = 'gaudirun.py -n -v -o %s %s' \
                   % (tmp_py.name, py_opts.name)
        opts_str = ''
        err_msg = ''
        options = {}

        rc, stdout, m = shellEnv_cmd(gaudirun, self.env)

        if stdout.find(b'Gaudi.py') >= 0:
            msg = 'The version of gaudirun.py required for your application is not supported.'
            raise ValueError(None, msg)

        elif stdout.find(b'no such option: -o') >= 0:
            gaudirun = 'gaudirun.py -n -v -p %s %s' % (tmp_pkl.name, py_opts.name)
            rc, stdout, m = shellEnv_cmd(gaudirun, self.env)
            rc = 0

            if stdout and rc == 0:
                opts_str = stdout
                err_msg = 'Please check %s -v %s' % (cmdbase, py_opts.name)
                err_msg += ' returns valid python syntax'

        else:
            cmd = 'gaudirun.py -n -p %s %s' % (tmp_pkl.name, py_opts.name)
            rc, stdout, m = shellEnv_cmd(cmd, self.env)
            if rc == 0 and stdout:
                opts_str = tmp_py.read()
                err_msg = 'Please check gaudirun.py -o file.py produces a ' \
                          'valid python file.'

        #We have to remove the defunct long representation for python 3
        opts_str = re.sub(r'(\d)L(\})', r'\1\2', opts_str)
        opts_str = re.sub(r'(\d)L(\,)', r'\1\2', opts_str)

        if stdout and rc == 0:
            try:
                options = eval(opts_str)
            except Exception as err:
                logger.error('Cannot eval() the options file. Exception: %s', err)
                from traceback import print_exc
                logger.error(' ', print_exc())
                raise ApplicationConfigurationError(stdout.decode() + '###SPLIT###' + m.decode())
            try:
                opts_pkl_string = tmp_pkl.read()
            except IOError as err:
                logger.error('Cannot read() the temporary pickle file: %s', tmp_pkl.name)
                raise err

        if not rc == 0:
            logger.debug('Failed to run: %s', gaudirun)
            raise ApplicationConfigurationError(stdout.decode() + '###SPLIT###' + m.decode())

        tmp_pkl.close()
        py_opts.close()
        tmp_py.close()
        return (options, opts_pkl_string)

    def _join_opts_files(self):
        '''Create a single options file from all supplied options.'''
        joined_py_opts = ''
        for name in self.optsfiles:
            try:
                this_file = open(expandfilename(name), 'r')
                import os.path
                if os.path.splitext(name)[1] == '.py':
                    joined_py_opts += this_file.read()
                elif os.path.splitext(name)[1] == '.opts':
                    joined_py_opts += 'from Gaudi.Configuration import *\n'
                    joined_py_opts += 'importOptions(\'' + name + '\')\n'
                else:
                    msg = 'Only extensions of type ".opts" and ".py" allowed'
                    raise GangaTypeError(msg)
            except IOError as err:
                logger.error('_join_opts_files Error: %s', str(err))
                logger.error('There was an IOError with the options file: %s', name)

        if self.extraopts:
            joined_py_opts += self.extraopts

        return joined_py_opts

    def get_input_data(self):
        '''Collects the user specified input data that the job will process'''
        data = []
        try:
            opts_input = self.opts_dict['EventSelector']['Input']
            data = [f for f in opts_input]
        except KeyError as err:
            logger.debug('No inputdata has been defined in the options file.')
            logger.debug("%s" % str(err))

        from GangaCore.GPIDev.Base.Filters import allComponentFilters
        file_filter = allComponentFilters['gangafiles']

        all_files = []
        for d in data:
            p1 = d.find('DATAFILE=') + len('DATAFILE=')
            quote = d[p1]
            p2 = d.find(quote, p1 + 1)
            f = d[p1 + 1:p2]
            this_file = file_filter(f, None)
            if this_file is None:
                this_file = LocalFile(name=f)
            all_files.append(this_file)

        ds = LHCbDataset(files=all_files, fromRef=True)
        return ds

    def get_output_files(self):
        '''Collects and organizes filenames that the job outputs'''

        sbtypes = GangaCore.Utility.Config.getConfig('LHCb')['outputsandbox_types']
        outsandbox = []
        outputdata = []

        if 'CounterSummarySvc' in self.opts_dict:
            if 'xmlfile' in self.opts_dict['CounterSummarySvc']:
                f = self.opts_dict['CounterSummarySvc']['xmlfile']
                if sbtypes.count('CounterSummarySvc') > 0:
                    outsandbox.append(f)
                else:
                    outputdata.append(f)

        datatypes = ['NTupleSvc', 'EvtTupleSvc']
        for this_type in datatypes:
            if this_type in self.opts_dict:
                if 'Output' in self.opts_dict[this_type]:
                    tuples = self.opts_dict[this_type]['Output']
                    # tuple output is returned as a list
                    for t in tuples:
                        f = t.split('\'')[1]
                        if sbtypes.count(this_type) > 0:
                            outsandbox.append(f)
                        else:
                            outputdata.append(f)

        if 'HistogramPersistencySvc' in self.opts_dict:
            if 'OutputFile' in self.opts_dict['HistogramPersistencySvc']:
                f = self.opts_dict['HistogramPersistencySvc']['OutputFile']
                if sbtypes.count('HistogramPersistencySvc') > 0:
                    outsandbox.append(f)
                else:
                    outputdata.append(f)

        datatypes = ['MicroDSTStream', 'DstWriter', 'GaussTape', 'DigiWriter']
        for this_type in datatypes:
            if(this_type in self.opts_dict):
                if('Output' in self.opts_dict[this_type]):
                    # get just the file name
                    this_file = self.opts_dict[this_type]['Output'].split('\'')[1]
                    if this_file.upper().startswith('PFN:'):
                        this_file = this_file[4:]
                    if sbtypes.count(this_type) > 0:
                        outsandbox.append(this_file)
                    else:
                        outputdata.append(this_file)

        return outsandbox, outputdata

    def get_output(self, job):
        '''Builds lists of output files and output data.'''

        outputdata = [f.namePattern for f in job.outputfiles if outputFilePostProcessingOnWN(job, getName(f)) ]
        outsandbox = [f.namePattern for f in job.outputfiles if not outputFilePostProcessingOnWN(job, getName(f)) ]

        # if user put any files in both, remove them from the sandbox
        for f in outsandbox:
            if outputdata.count(f) != 0:
                outsandbox.remove(f)
                msg = 'User placed the file %s in both the outputsandbox and '
                msg += 'outputdata. It will be removed from the sandbox.'
                logger.warning(msg, f)

        gaudi_outsandbox, gaudi_outputdata = self.get_output_files()

        # handle (as best we can) any user supplied wildcards
        datalist = []  # files in sandbox that match pattern in data
        for f in outputdata:
            datalist += fnmatch.filter(gaudi_outsandbox, f)

        sandlist = []  # files in data that match sandbox pattern
        for f in outsandbox:
            sandlist += fnmatch.filter(gaudi_outputdata, f)

        datadatalist = []  # files in data that match patterns in data
        for f in outputdata:
            datadatalist += fnmatch.filter(gaudi_outputdata, f)

        # files in sandbox which match patterns in data -> data
        for f in datalist:
            gaudi_outputdata.append(f)
            if f in gaudi_outsandbox:
                gaudi_outsandbox.remove(f)
        # files in data which match patterns in sandbox but not data -> sandbox
        for f in sandlist:
            if datalist.count(f) == 0 and datadatalist.count(f) == 0:
                gaudi_outsandbox.append(f)
                if f in gaudi_outputdata:
                    gaudi_outputdata.remove(f)

        outsandbox += gaudi_outsandbox
        outputdata += gaudi_outputdata

        return unique(outsandbox), unique(outputdata)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


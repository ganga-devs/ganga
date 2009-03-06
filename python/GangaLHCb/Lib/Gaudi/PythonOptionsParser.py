#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Uses gaudirun.py to parse the job options file to allow for easy extraction
of inputdata, outputdata and output files.'''

import tempfile, fnmatch
from Ganga.GPIDev.Lib.File import FileBuffer
import Ganga.Utility.logging
from Ganga.Utility.util import unique
import Ganga.Utility.Config 
from GangaLHCb.Lib.LHCbDataset import LHCbDataset, LHCbDataFile
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import collect_lhcb_filelist
from Ganga.Core import ApplicationConfigurationError
from Ganga.Utility.files import expandfilename

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class PythonOptionsParser:
    """ Parses job options file(s) w/ gaudirun.py to extract user's files"""
    
    def __init__( self, optsfiles, extraopts, shell):
        self.optsfiles = optsfiles
        self.extraopts = extraopts
        self.shell = shell
        self.opts_dict,self.opts_pkl_str = self._get_opts_dict_and_pkl_string()

    def _get_opts_dict_and_pkl_string( self):
        '''Parse the options using gaudirun.py and create a dictionary of the
        configuration and pickle the options. The app handler will make a copy
        of the .pkl file for each job.'''
        tmp_pkl = tempfile.NamedTemporaryFile( suffix = '.pkl')
        tmp_py = tempfile.NamedTemporaryFile( suffix = '.py')
        py_opts = tempfile.NamedTemporaryFile( suffix = '.py')
        py_opts.write( self._join_opts_files())
        py_opts.flush()

        gaudirun = 'gaudirun.py -n -v -o %s %s' \
                   % (tmp_py.name, py_opts.name)
        opts_str = ''
        err_msg = ''
        options = {}
        
        rc, stdout, m = self.shell.cmd1( gaudirun)
        
        if stdout.find('no such option: -o') >= 0:
            # old version of gaudirun.py
            gaudirun = 'gaudirun.py -n -v -p %s %s' \
                       % ( tmp_pkl.name, py_opts.name)
            rc, stdout, m = self.shell.cmd1( gaudirun)

            if stdout and rc == 0:
                opts_str = stdout
                err_msg = 'Please check gaudirun.py -n -v %s' % py_opts.name
                err_msg += ' returns valid python syntax' 
        else:
            # new version of gaudirun.py
            cmd = 'gaudirun.py -n -p %s %s' % (tmp_pkl.name, py_opts.name)
            rc, stdout, m = self.shell.cmd1(cmd)
            if rc == 0 and stdout:
                opts_str = tmp_py.read()
                err_msg = 'Please check gaudirun.py -o file.py produces a ' \
                          'valid python file.'

        if stdout and rc == 0:
            try:
                options = eval(opts_str)
            except Exception, e:
                logger.error('Cannot eval() the options file. Exception: %s',e)
                from traceback import print_exc
                logger.error(' ', print_exc())
                raise ApplicationConfigurationError(None,stdout)
            try:
                opts_pkl_string = tmp_pkl.read()        
            except IOError, e:
                logger.error('Cannot read() the temporary pickle file: %s',
                             tmp_pkl.name)
        
        if not rc ==0:
            logger.debug('Failed to run: %s', gaudirun) 
            raise ApplicationConfigurationError(None,stdout)
                                
        tmp_pkl.close()
        py_opts.close()
        return (options, opts_pkl_string)
        
    def _join_opts_files( self):
        '''Create a single options file from all supplied options.'''
        joined_py_opts = ''
        for name in self.optsfiles:
            try:
                file = open( expandfilename(name),'r')
                import os.path
                if os.path.splitext( name)[1] == '.py':
                    joined_py_opts += file.read()
                elif os.path.splitext( name)[1] == '.opts':
                    joined_py_opts += 'from Gaudi.Configuration import *\n'
                    joined_py_opts += 'importOptions(\'' + name + '\')\n'
                else:
                    msg = 'Only extensions of type ".opts" and ".py" allowed'
                    raise TypeError(msg)
            except IOError, e:
                logger.error('%s',e)
                logger.error('There was an IOError with the options file: %s',
                             name)
                    
        if self.extraopts:
            joined_py_opts += self.extraopts

        return joined_py_opts

    def get_input_data( self):
        '''Collects the user specified input data that the job will process'''
        data = []
        try:
            data = [f for f in self.opts_dict['EventSelector']['Input']]
        except KeyError, e:
            logger.debug('No inputdata has been defined in the options file.')
        
        splitFiles = [x.split('\'')[1] for x in data]
        lb = LHCbDataset()
        for f in splitFiles:
            d = LHCbDataFile()
            d.name = f
            lb.files.append(d)
        return lb

    def get_output_files( self):        
        '''Collects and organizes filenames that the job outputs'''
        
        sbtypes = Ganga.Utility.Config.getConfig('LHCb')['outputsandbox_types']
        outsandbox = []
        outputdata = []
        
        if self.opts_dict.has_key('NTupleSvc'):
            if self.opts_dict['NTupleSvc'].has_key('Output'):
                tuples = self.opts_dict['NTupleSvc']['Output']
                # tuple output is returned as a list 
                for t in tuples:
                    f = t.split('\'')[1]
                    if sbtypes.count('NTupleSvc') > 0:
                        outsandbox.append(f)
                    else:
                        outputdata.append(f)

        if self.opts_dict.has_key('HistogramPersistencySvc'):
            if self.opts_dict['HistogramPersistencySvc'].has_key('OutputFile'):
                 f = self.opts_dict['HistogramPersistencySvc']['OutputFile']
                 if sbtypes.count('HistogramPersistencySvc') > 0:
                     outsandbox.append(f)
                 else:
                     outputdata.append(f)                

        datatypes = ['MicroDSTStream','DstWriter','GaussTape','DigiWriter']
        for type in datatypes:
            if(self.opts_dict.has_key(type)):
                if(self.opts_dict[type].has_key('Output')):
                    # get just the file name
                    file = self.opts_dict[type]['Output'].split('\'')[1]
                    if file.startswith('PFN:') or file.startswith('pfn:'):
                        file = file[4:]
                    if sbtypes.count(type) > 0:
                        outsandbox.append(file)
                    else:
                        outputdata.append(file)

        return outsandbox, outputdata

    def get_output(self, job):
        '''Builds lists of output files and output data.'''

        outputdata = collect_lhcb_filelist(job.outputdata)
        outsandbox = [f for f in job.outputsandbox]

        # if user put any files in both, remove them from the sandbox
        for f in outsandbox:
            if outputdata.count(f) != 0:
                outsandbox.remove(f)
                msg = 'User placed the file %s in both the outputsandbox and '
                msg += 'outputdata. It will be removed from the sandbox.'
                logger.warning(msg,f)
        
        gaudi_outsandbox,gaudi_outputdata = self.get_output_files()

        # handle (as best we can) any user supplied wildcards
        datalist = [] # files in sandbox that match pattern in data
        for f in outputdata:
            datalist += fnmatch.filter(gaudi_outsandbox,f)
            
        sandlist = [] # files in data that match sandbox pattern
        for f in outsandbox:
            sandlist += fnmatch.filter(gaudi_outputdata,f)

        datadatalist = [] # files in data that match patterns in data
        for f in outputdata:
            datadatalist += fnmatch.filter(gaudi_outputdata,f)
            
        # files in sandbox which match patterns in data -> data
        for f in datalist:
            gaudi_outputdata.append(f)
            gaudi_outsandbox.remove(f)
        # files in data which match patterns in sandbox but not data -> sandbox
        for f in sandlist:
            if datalist.count(f) == 0 and datadatalist.count(f) == 0:
                gaudi_outsandbox.append(f)
                gaudi_outputdata.remove(f)

        outsandbox += gaudi_outsandbox
        outputdata += gaudi_outputdata

        return unique(outsandbox), unique(outputdata)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

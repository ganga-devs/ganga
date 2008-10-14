#! /usr/bin/env python
# $Id: PythonOptionsParser.py,v 1.6 2008-09-03 11:54:59 wreece Exp $

__author__ = 'Greig A Cowan'
__date__ = 'June 2008'
__version__ = 0.3

'''
Uses gaudirun.py to parse the job options file to allow for easy extraction of inputdata,
outputdata and output files.
'''

import tempfile
from Ganga.GPIDev.Lib.File import FileBuffer
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class PythonOptionsParser:

    def __init__( self, optsfiles, extraopts, shell):
        self.optsfiles = optsfiles
        self.extraopts = extraopts
        self.shell = shell
        self.opts_dict, self.opts_pkl_str = self._get_opts_dict_and_pkl_string()


    def _get_opts_dict_and_pkl_string( self):
        '''Parse the options using gaudirun.py and create a dictionary of the configuration
        and pickle the options. The app handler will make a copy of the .pkl file for each job.'''
        tmp_pkl = tempfile.NamedTemporaryFile( suffix = '.pkl')
        py_opts = tempfile.NamedTemporaryFile( suffix = '.py')
        py_opts.write( self._join_opts_files())
        py_opts.flush()
        
        gaudirun = 'gaudirun.py -n -v -p %s %s' % ( tmp_pkl.name, py_opts.name)
        outputString = ''
        options = {}
        
        rc, optionsString, m = self.shell.cmd1( gaudirun)
        if not rc ==0:
            from Ganga.Core import ApplicationConfigurationError
            raise ApplicationConfigurationError( None, 'Problem with syntax in options file')
            logger.error('Cannot run: %s', gaudirun)
    
        if optionsString and rc == 0:
            try:
                options = eval( optionsString)
            except Exception, e:
                logger.error('Cannot eval() the options file. Exception: %s', e)
            try:
                opts_pkl_string = tmp_pkl.read()        
            except IOError, e:
                logger.error('Cannot read() the temporary pickle file: %s', tmp_pkl.name)
    
        tmp_pkl.close()
        py_opts.close()
        return (options, opts_pkl_string)
    
    
    def _join_opts_files( self):
        '''Create a single options file from all supplied options.'''
        joined_py_opts = ''
        for name in self.optsfiles:
            try:
                file = open( name,'r')
                import os.path
                if os.path.splitext( name)[1] == '.py':
                    joined_py_opts += file.read()
                elif os.path.splitext( name)[1] == '.opts':
                    joined_py_opts += 'from Gaudi.Configuration import *\n'
                    joined_py_opts += 'importOptions(\'' + name + '\')\n'
                else:
                    raise TypeError('Only extensions of type ".opts" and ".py" allowed')
            except IOError, e:
                logger.error('There was an IOError with the options file: %s', name)
                    
        if self.extraopts:
            joined_py_opts += self.extraopts

        return joined_py_opts


    def get_input_data( self):
        '''Collects the user specified input data that the job will process'''
        data = []
        #inputdata = []
        try:
            data = [f for f in self.opts_dict['EventSelector']['Input']]
        except KeyError, e:
            logger.debug('No inputdata has been defined in the options file.')
        
        #for datum in data:
        #   # remove PFN: from filename
        #    if datum.startswith('PFN:') or datum.startswith('pfn:'):
        #        inputdata.append( datum[4:])
        #    else:
        #        inputdata.append( datum)

        from GangaLHCb.Lib.LHCbDataset import LHCbDataset, LHCbDataFile
        lb = LHCbDataset()
        splitFiles = [x.split('\'')[1] for x in data]
        lb = LHCbDataset()
        for f in splitFiles:
            d = LHCbDataFile()
            d.name = f
            lb.files.append(d)
        return lb

    def get_output_files( self):
        '''Collects the ntuple and histogram filenames that the job outputs'''
        outputfiles = []
        tuple = ''
        histo = ''
        try:
            tuple = self.opts_dict['NTupleSvc']['Output'][0].split('\'')[1]
        except KeyError, e:
            logger.debug('No NTupleSvc is defined: %s', e)
        
        try:
            histo = self.opts_dict['HistogramPersistencySvc']['OutputFile']
        except KeyError, e:
            logger.debug('No HistogramPersistencySvc is defined: %s', e)

        if tuple: outputfiles.append( tuple)
        if histo: outputfiles.append( histo)

        if outputfiles:
            logger.info('Found these histograms and NTuples: %s', str(outputfiles))
        return outputfiles

    def get_output_data( self):
        '''If the job outputs dsts, digi or sim files, then get their names'''

        datatypes = ['DstWriter', 'GaussTape', 'DigiWriter']
        data = []
        outputdata = []

        try:
            data = [self.opts_dict[ type].Output.split('\'')[1] for type in datatypes]
        except KeyError, e:
            logger.debug('There is no output file of type %s defined in the options', e)
        except AttributeError, e:
            logger.debug('There is no output file of type %s defined in the options', e)
        
        if data:
            logger.info("Found these %s files: %s", (type, '\n'.join(data)))
        for datum in data:
            # remove PFN: from filename
            if datum.startswith('PFN:') or datum.startswith('pfn:'):
                outputdata.append( datum[4:])
            else:
                outputdata.append( datum)

        return outputdata

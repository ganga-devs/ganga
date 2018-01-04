'''This class defines the number of subjobs per use case'''

from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Adapters.ISplitter import ISplitter
from GangaCore.GPIDev.Schema import *
import GangaCore.Utility.logging


class SBSubmission(ISplitter):
    '''Bulk submission management class: define number of subjobs per use case.
    -> Personal production use case: user should define the number of subjobs;
    -> Official and personal production analysis use case: subjobs number depends on the number of input file lists;
    -> Free analysis use case: the number of subjobs is defined by user;'''
    
    _name = "SBSubmission"
    _schema = Schema(Version(1,0), {})
    
    def split(self, job):
        '''The method creates and returns an array of subjobs starting from the job passed as parameter'''
        logger = GangaCore.Utility.logging.getLogger()
        logger.debug('SBSubmission split called.')
        subjobs = []
        
        # check the input mode (dir, none or list)
        if job.inputdata.input_mode in ('dir', 'none'):
            # in dir or none mode, user has to define the desired number of subjobs
            if job.inputdata.number_of_subjobs <= 0:
                raise ApplicationConfigurationError('You must define the number of subjobs.')
            for i in xrange(job.inputdata.number_of_subjobs):
                j = self.createSubjob(job)
                subjobs.append(j)
        elif job.inputdata.input_mode == 'list':
            # in list mode user has to define one or more input paths (i.e. one or more files): each list file contains
            # a list of path of files that become the input of a job
            if len(job.inputdata.input_path) <= 0:
                raise ApplicationConfigurationError('You must define an input file list.')
            # for each list file (i.e. each element in input_path array) a subjob will be created
            for f in job.inputdata.input_path:
                j = self.createSubjob(job)
                j.inputsandbox = [f]
                subjobs.append(j)
        else:
            raise ApplicationConfigurationError('input_mode not recognized.')
        
        logger.debug('%d subjobs created.' % len(subjobs))
        return subjobs

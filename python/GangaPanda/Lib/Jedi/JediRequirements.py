from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class JediRequirements(GangaObject):
    '''Helper class to group Jedi requirements.
    '''

    _schema = Schema(Version(1,1), {
        'long'          : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Send job to a long queue'),
        'cloud'         : SimpleItem(defvalue='US',protected=0,copyable=1,doc='cloud where jobs are submitted (default:US)'),
        'anyCloud'      : SimpleItem(defvalue=True,protected=0,copyable=1,doc='Set to true to allow jobs to run in all clouds. If False, jobs are limited to run in "requirements.cloud"'),
        'memory'        : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required memory size'),
        'cputime'       : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required CPU count in seconds. Mainly to extend time limit for looping detection'),
        'corCheck'      : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Enable a checker to skip corrupted files'),        
        'notSkipMissing': SimpleItem(defvalue=False,protected=0,copyable=1,doc='If input files are not read from SE, they will be skipped by default. This option disables the functionality'),
        'excluded_sites': SimpleItem(defvalue = [],typelist=['str'],sequence=1,protected=0,copyable=1,doc='Panda sites to be excluded while brokering.'),
        'excluded_clouds': SimpleItem(defvalue = [],typelist=['str'],sequence=1,protected=0,copyable=1,doc='Clouds to be excluded while brokering.'),
        'express'       : SimpleItem(defvalue = False,protected=0,copyable=1,doc='Send the job using express quota to have higher priority. The number of express subjobs in the queue and the total execution time used by express subjobs are limited (a few subjobs and several hours per day, respectively). This option is intended to be used for quick tests before bulk submission. Note that buildXYZ is not included in quota calculation. If this option is used when quota has already exceeded, the panda server will ignore the option so that subjobs have normal priorities. Also, if you submit 1 buildXYZ and N runXYZ subjobs when you only have quota of M (M < N),  only the first M runXYZ subjobs will have higher priorities'),
        'enableJEM'     : SimpleItem(defvalue = False,protected=0,copyable=1,doc='Enable the Job Execution Monitor.'),
        'configJEM'     : SimpleItem(defvalue = '',protected=0,copyable=1,doc='Config string for the Job Execution Monitor.'),
        'enableMerge'   : SimpleItem(defvalue = False, protected=0, copyable=1, doc='Enable the output merging jobs.'),
        'configMerge'   : SimpleItem(defvalue = {'type':'','exec':''}, protected=0, copyable=1, doc='Config parameters for output merging jobs.'),
        'usecommainputtxt' : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Boolean if input.txt contains the input files as comma separated list or separeted by a line break'),
        'rootver' : SimpleItem(defvalue = '',protected=0,copyable=1,doc='Specify a different root version for non-Athena jobs.'),
        'overwriteQueuedata'     : SimpleItem(defvalue = False,protected=0,copyable=1,doc='Expert option: overwriteQueuedata.'),
        'overwriteQueuedataConfig'     : SimpleItem(defvalue = '',protected=0,copyable=1,doc='Expert option: overwriteQueuedataConfig.'),
        'disableAutoRetry' : SimpleItem(defvalue=False,protected=0,copyable=1,doc='disable automatic job retry on the server side'),
        'noEmail' : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Suppress email notification'),
        'skipScout' : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Skip scout jobs'),
        'split'         : SimpleItem(defvalue=0,typelist=['type(None)','int'],copyable=1,doc='Number of sub-jobs to which a job is split.'),
        'nFilesPerJob'  : SimpleItem(defvalue=0,typelist=['type(None)','int'],copyable=1,doc='Number of files on which each sub-job runs.'),
        'nEventsPerFile'  : SimpleItem(defvalue=0,typelist=['type(None)','int'],copyable=1,doc='Number of events per file to run over'),
        'nGBPerJob'  : SimpleItem(defvalue=0,typelist=['type(None)','int'],copyable=1,doc='Instantiate one sub job per NGBPERJOB GB of input files. nGBPerJob=MAX sets the size to the default maximum value'),
        'maxNFilesPerJob'  : SimpleItem(defvalue=200,typelist=['type(None)','int'],copyable=1,doc='The maximum number of files per job is 200 by default since too many input files result in a too long command-line argument on WN which crashes the job. This option relax the limit.'),
    })

    _category = 'JediRequirements'
    _name = 'JediRequirements'
    _defcloud = ''
    
    def __init__(self):
        super(JediRequirements,self).__init__()
        from pandatools import PsubUtils
        import sys
        sys.stdout = open('/dev/null','w')
        sys.stderr = open('/dev/null','w')

        # cache the call to getCloudUsingFQAN as otherwise it can be slow
        if JediRequirements._defcloud == '':
            JediRequirements._defcloud = PsubUtils.getCloudUsingFQAN(None,False)
        self.cloud = self._defcloud

        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        if not self.cloud:
            import random
            from pandatools import Client
            clouds = Client.PandaClouds.keys()
            clouds.remove('OSG')
            self.cloud = random.choice(clouds)

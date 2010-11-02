from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class PandaRequirements(GangaObject):
    '''Helper class to group Panda requirements.
    '''

    _schema = Schema(Version(1,3), { 
        'long'          : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Send job to a long queue'),
        'cloud'         : SimpleItem(defvalue='US',protected=0,copyable=1,doc='cloud where jobs are submitted (default:US)'),
        'anyCloud'      : SimpleItem(defvalue=True,protected=0,copyable=1,doc='Set to true to allow jobs to run in all clouds. If False, jobs are limited to run in "requirements.cloud"'),
        'memory'        : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required memory size'),
        'cputime'       : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required CPU count in seconds. Mainly to extend time limit for looping detection'),
        'corCheck'      : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Enable a checker to skip corrupted files'),        
        'notSkipMissing': SimpleItem(defvalue=False,protected=0,copyable=1,doc='If input files are not read from SE, they will be skipped by default. This option disables the functionality'),
        'excluded_sites': SimpleItem(defvalue = [],typelist=['str'],sequence=1,protected=0,copyable=1,doc='Panda sites to be excluded while brokering.'),
        'express'       : SimpleItem(defvalue = False,protected=0,copyable=1,doc='Send the job using express quota to have higher priority. The number of express subjobs in the queue and the total execution time used by express subjobs are limited (a few subjobs and several hours per day, respectively). This option is intended to be used for quick tests before bulk submission. Note that buildXYZ is not included in quota calculation. If this option is used when quota has already exceeded, the panda server will ignore the option so that subjobs have normal priorities. Also, if you submit 1 buildXYZ and N runXYZ subjobs when you only have quota of M (M < N),  only the first M runXYZ subjobs will have higher priorities'),
    })

    _category = 'PandaRequirements'
    _name = 'PandaRequirements'
    
    def __init__(self):
        super(PandaRequirements,self).__init__()
        from pandatools import PsubUtils
        import sys
        sys.stdout = open('/dev/null','w')
        sys.stderr = open('/dev/null','w')
        self.cloud = PsubUtils.getCloudUsingFQAN(None,False)
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        if not self.cloud:
            import random
            from pandatools import Client
            clouds = Client.PandaClouds.keys()
            clouds.remove('OSG')
            self.cloud = random.choice(clouds)

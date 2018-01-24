#Clean

from GangaRobot.Framework.Action import IAction
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.Config import getConfig
import os, shutil
from os.path import join
from GangaRobot.Framework.exceptions import *

logger = getLogger()

class Publisher(IAction):
    """
    Publisher IAction implementation. This action will take the test results
    and pack them up in a .tgz archive in a published location. All the test
    files are renamed to have the location (site) of the tests included.
    """
    
    def execute(self, runid):
        logger.info("Starting publication")
        from GangaCore.Utility.files import expandfilename
        config = getConfig('TestRobot')
        topdir=expandfilename(config['JobDir'])
        publishPath=expandfilename(config['PublishPath'])
        version = config['VersionNumber']
        site = config['Site']
        for i in range(len(config['TestPairs'])):
            jobdir = join(topdir, version+"_Job_"+str(i))
            import tarfile
            test=config['TestPairs'][i][0].replace('/','.')
            option=config['TestPairs'][i][2]
            fname = version+'_'+test+'_'+option+'-'+site+'.tgz'
            archive = tarfile.open(join(publishPath,fname),'w:gz')
            archive.posix=False
            for file in [f for f in os.listdir(join(jobdir,'output'))
                         if os.path.isfile(join(jobdir,'output',f))]:
                toname= file.replace(option,option+'-'+site)
                
                archive.add(join(jobdir,'output',file),join('output',toname))
            for file in [f for f in os.listdir(join(jobdir,'reports','latest'))
                         if os.path.isfile(join(jobdir,'reports','latest',f))]:
                toname = file.replace(option,option+'-'+site)
                archive.add(join(jobdir,'reports','latest',file),join(toname))
            archive.close()

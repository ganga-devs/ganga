"""
Tester.py

GangaRobot IACtion derived class for the TestRobot option:
reads the jobs from the configuration file, and starts a
new instance of the new ganga to test them

"""
from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from Ganga.Utility.logging import getLogger
from Ganga.GPI import *
import Ganga.Utility.Config
import os, datetime, time, shutil
from os.path import join
from GangaRobot.Framework.exceptions import *

logger = getLogger()

class Tester(IAction):

    """ Tester implementation

    Starts up an new pre-release of ganga and tests it

    """

    #class variables

    def execute(self, runid):

        jobsList = []

        config = Ganga.Utility.Config.getConfig('TestRobot')
        OptionList = config['TestPairs']
        InstallDir = config['InstallPath']
        JobDir = config['JobDir']
        ReleaseNo = config['VersionNumber']
        Delta = int(config['JobTimeOut'])
        logger.info("Job Time-out at %s",config['JobTimeOut'])
        SleepSecs = 30
      
        if os.environ.has_key('GANGA_INTERNAL_PROCEEXEC'):
            try:
                del os.environ['GANGA_INTERNAL_PROCEEXEC']
                logger.info("deleted GANGA_INTERNAL_PROCEEXEC")
            except Exception, e:
                raise GangaRobotBreakError("Could not delete GANGA_INTERNAL_PROCEEXEC with error: %s" %e)
        else:
            logger.info("GANGA_INTERNAL_PROCEEXEC does not exist")
            pass
        
        i = 0
        for i in range(len(OptionList)):
            TestOption = OptionList[i][0]
            TestBackend = OptionList[i][1]
            TestConfig = OptionList[i][2]
            #make job directory stuff
            Jobfolder = os.path.join(JobDir,ReleaseNo+"_Job_"+str(i))
            try:
                os.mkdir(Jobfolder)
            except OSError, e:
                logger.info(e)
                shutil.rmtree(Jobfolder)
                os.mkdir(Jobfolder)
            #create inifile
            JobIniFile = self._writeinifile(Jobfolder)
            j = Job()
            BackendList = plugins("backends")
            BackendFound = False
            for i in range(len(BackendList)):
                if TestBackend == BackendList[i]:
                    j.backend = BackendList[i]
                    BackendFound = True
                else:
                    pass
            if not BackendFound:
                break # DO SOMETHING ELSE HERE
            j.application.exe = InstallDir+"/install/"+ReleaseNo+"/bin/ganga"
            #j.application.exe = "ganga"            - DEBUGGING LINE
            j.application.args = ["--test","--config:"+TestConfig+".ini",TestOption]
            j.application.env = { 'GANGA_CONFIG_PATH':JobIniFile }
            logger.info(j.application)
            jobsList += [j]

        #k = Job()
        #k.application.exe = InstallDir+"/install/"+ReleaseNo+"/bin/ganga"
        #k.application.args = ["--test","-o[TestingFramework]EnableHTMLReporter=True","-oEnableTestRunner=False"]
        #jobsList += [k]

        i = 0
        self.TestsAllGood = True
        for i in range(len(jobsList)):
            try:
                jobsList[i].submit()
                StartTime = datetime.datetime.now()
                #logger.info("Submitted job "+OptionList[i][0]+" to "+OptionList[i][1]+" backend")
                #Now to wait for time-out
                JobFinished = False
                while not JobFinished:

                    logger.info("sleeping for %s secs" % str(SleepSecs))
                    time.sleep(SleepSecs)
                    logger.info("finished sleeping")
                    
                    if jobsList[i].status == 'completed':
                        JobFinished = True
                    elif jobsList[i].status == 'failed':
                        JobFinished = True
                    elif (jobsList[i].status == 'running') or (jobsList[i].status == 'submitted'):
                        EndTime = StartTime + datetime.timedelta(seconds=Delta)
                        NowTime = datetime.datetime.now()
                        if (NowTime > EndTime):
                            NowTimeAsString = datetime.datetime.now().strftime("%d %b %Y %H:%M:%S")
                            logger.info("Job "+str(i)+" has timed out at %s" % NowTimeAsString)
                            jobsList[i].kill()
                            JobFinished = True
                    else:
                        pass
                    # PUT HEARTBEAT HERE
                    try:
                        self._HeartBeat()
                    except:
                        raise GangaRobotFatalError
   
            except:
                logger.error("Failed to submit job "+str(i)+" Test option %s, TestConfig %s",TestOption, TestConfig)
                self.TestsAllGood = False

        logger.info("All jobs finished")
        logger.info(jobs)
        #for j in jobs:
            #j.peek('stdout')
        jobs.remove()
        
        if self.TestsAllGood:
            self._WriteFinalState()
            
        self._cleanup
        
        
    def _writeinifile(self, Jobfolder):
        JobConfigFile = os.path.join(Jobfolder, "TestConfig.ini")
        f = open(JobConfigFile, 'w')
        f.write("[TestingFramework]\n")
        f.write("EnableHTMLReporter = True\n")
        f.write("ReleaseTesting = True\n")
        JobLogOutputDir = os.path.join(Jobfolder,"output")
        f.write("LogOutputDir = "+JobLogOutputDir+"\n")
        f.write("OutputDir = "+Jobfolder+"\n")
        JobReportsOutputDir = os.path.join(Jobfolder,"reports")
        f.write("ReportsOutputDir = "+JobReportsOutputDir+"\n")
        f.write("Config=Imperial.ini")
        f.write("\n")
        f.write("[Configuration]\n")
        f.write("gangadir = "+Jobfolder+"\n")
        f.write("\n")
        #f.write("[Local]\n")
        #f.write("remove_workdir = False\n")
        f.flush()
        f.close()
        return JobConfigFile
        
    def _cleanup(self):
        # remove installed files.
        pass
        
        
    def _WriteFinalState(self):
        import tempfile, fnmatch
        tmpdir = tempfile.gettempdir()
        config = Ganga.Utility.Config.getConfig('TestRobot')
        CurrentVersionData = config['VersionNumber']+" - "+config['VersionTime']
        gangaconfig = Ganga.Utility.Config.getConfig('Configuration')
        testrobotdir = gangaconfig['gangadir']
        VersionFileFound = False
        lastversionfile = str(testrobotdir)+os.sep+"LastVersionFile.txt"
        for file in os.listdir(testrobotdir):
            if fnmatch.fnmatch(file, 'LastVersionFile.txt'):
                f = open(file, 'r')
                LastVersionData = f.read()
                f.close()
                VersionFilefound = True
        # Test for Version File
        if (VersionFileFound == True):
            #compare with current data
            if (LastVersionData != CurrentVersionData):
                # another version has been installed and tested
                f = open(lastversionfile, 'w')
                f.write(CurrentVersionData+"\n")
                f.flush()
                f.close()
                try:
                    shutil.move('LastVersionTemp.txt','LastVersionFile.txt')
                except:
                    GangaRobotContinueError("Failed to move LastVersionFile")
        else:
            f = open(lastversionfile,'w')
            f.write(CurrentVersionData)
            f.flush()
            f.close()

    def _HeartBeat(self):
        config = Ganga.Utility.Config.getConfig('Configuration')
        heartbeatfile = str(config['gangadir'])+os.sep+"heartbeat.txt"
        heartbeattmpfile = str(config['gangadir'])+os.sep+"heartbeattemp.txt"
        try:
            f = open(heartbeatfile,'r')
            HeartBeatData = (f.read())[:-7].strip()
            f.close()
            HBtime = HeartBeatData[:15].strip()
            HeartBeatTime = datetime.datetime(*(time.strptime(HBtime,"%H:%M:%S %j %y")[0:6]))
            if ((datetime.datetime.now() - HeartBeatTime) > datetime.timedelta(minutes=10)):
                f = open(heartbeattmpfile, 'w')
                f.write(datetime.datetime.now().strftime("%H:%M:%S %j %y")+" - "+str(os.getpid()))
                f.close()
                try:
                    shutil.move(heartbeattmpfile,heartbeatfile)
                except:
                    raise GangaRobotContinueError("Failed to move heartbeat file")            
        except IOError, e:
            f  = open(heartbeatfile, 'w')
            f.write(datetime.datetime.now().strftime("%H:%M:%S %j %y")+" - "+str(os.getpid()))
            f.close()
#


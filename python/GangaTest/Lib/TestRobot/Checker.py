"""CheckForPreRelease  IAction implementation"""

from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from GangaRobot.Framework.exceptions import *
from Ganga.Utility.logging import getLogger
import os, urllib, datetime, subprocess, time, fnmatch, shutil
#from Ganga.Utility.Config import getConfig
import Ganga.Utility.Config
#from GangaTest.Lib.TestRobot.Utils import Emailer

logger = getLogger()


class Checker(IAction):

    """ Checker implementation

    is setup to check a specified site for a new prerelease
    every 10 minutes.
    
    """

    #class variables
#    LastModifiedTime = ''
#    LastModified = -1
#    DownloadURL = ''
#    LastRelease = ''
#    cfgfile = ''
#    configfilename = 'JointCFG.ini'
#    NewRelease = False

    def execute(self, runid):
        #Get configuration data
        config = Ganga.Utility.Config.getConfig('TestRobot')
        self._getconfig()
        #Email on Startup
        if (self.EmailOnStartup == True):
            subject = "Ganga - TestRobot automatic restart by crontab"
            text = "GangaTestRobot restarted on: %s" %(datetime.datetime.now().strftime("%H:%M:%S %d %b %Y"))
            self._emailer(subject, text)
        else:
            pass
        self.NewRelease = False
        while not self.NewRelease:
            self._heartbeat()
            #Then check for update
            CurrentTime = datetime.datetime.now()
            SleepDelta = datetime.timedelta(seconds=int(self.SleepTime))
            if (CurrentTime - self.LastCheckedTime) < SleepDelta:
                logger.debug("Last check was too recent")
                pass
            else:
                logger.debug("Checking for update...")
                self.NewRelease = self._checkfornewrelease()
                self.LastCheckedTime = datetime.datetime.now()
                config.setSessionValue('LastCheckedTime',self.LastCheckedTime.strftime("%d %b %Y %H:%M:%S"))
            if self.NewRelease:
                config.setSessionValue('VersionNumber',self.VersionNumber)
                config.setSessionValue('VersionTime',self.VersionTime.strftime("%d %b %Y %H:%M:%S"))
                break
            logger.debug("No update available, sleeping for %s" %(self.SleepTime))
            time.sleep(int(self.SleepTime))
            #write heartbeat
            self.NewRelease = False # Reset NewRelease flag

    def _getconfig(self):
       
        config = Ganga.Utility.Config.getConfig('TestRobot')
        self.DownloadURL = config['ReleasePath']
        self.SleepTime = config['SleepTime']
        self.EmailOnStartup = config['EmailOnStartup']
        self.LastCheckedTime = config['LastCheckedTime']
        if ( self.LastCheckedTime == '0' ): 
            self.LastCheckedTime = self.InitialTime
        else:
            self.LastCheckedTime = datetime.datetime(*(time.strptime(self.LastCheckedTime,"%d %b %Y %H:%M:%S")[0:6]))
        # Get data from temp files
        self._getfiledata()

    def _getfiledata(self):
        gangaconfig = Ganga.Utility.Config.getConfig('Configuration')
        gangadirname = gangaconfig['gangadir']
        self.LastVersionData = 'None'
        #for file in os.listdir(gangadirname):
        #    if fnmatch.fnmatch(file, 'LastVersionFile.txt'):
        filename = gangadirname+os.sep+"LastVersionFile.txt"
        try:
            f = open(filename)
            self.LastVersionData = f.readline()
            f.close()
        except IOError as e:
            logger.info(e)
            pass
        #dirs below will have been deleted on clean up
        config = Ganga.Utility.Config.getConfig('TestRobot')
        #Set Install path with new directory  
        dirname = os.path.join(gangadirname, "Releases")
        try:
            os.mkdir(dirname)
        except OSError as e:
            logger.info(e)
        config.setSessionValue('InstallPath',dirname)
        #Sets job path
        dirname = os.path.join(gangadirname,'Jobs')
        try:
            os.mkdir(dirname)
        except OSError as e:
            logger.info(e)
        config.setSessionValue('JobDir',dirname)
        #return last version data
            
        
    def _checkfornewrelease(self):
    
           #obtain file
        try:
            f, inf = urllib.urlretrieve(self.DownloadURL+"/VERSIONS.txt")
        except:
            msg = "Failed to retrieve VERSIONS.txt from %s" % (self.DownloadURL)
            logger.error(msg)
            raise GangaRobotBreakError(msg,IOError)
        label = 'Content-Type'
        type = 'text/plain'
        for h in inf.headers:
            if (h.find(label) != -1):
                if (h.find(type) != -1):
                    # Found plain text file - this is what we want
                    pass
                else:
                    # Found other file - wrong place raise error
                    msg = "File not found or of wrong type at '%s'" % (self.DownloadURL+"VERSIONS")
                    raise GangaRobotBreakError(None,msg)
        #read lines into list
        f = open(f)
        VersionList = [v for v in f.readlines() if len(v)>2]
        logger.debug(VersionList)
        #find matching point
        #else return test first new
        i = -1
        match = -1
        for data in VersionList:
            i += 1
            if (data == self.LastVersionData) :
                match = i
                break
        #if none found, return first in list
        if (match == -1):
            self.VersionData = VersionList[0]
            self.VersionNumber = self._getversioninfo(self.VersionData)[0]
            self.VersionTime = self._getversioninfo(self.VersionData)[1]
            logger.debug("No matches found. Testing first in list")
            return True
        elif (match == (len(VersionList)-1)):
            self.VersionData = VersionList[(i-1)]
            logger.debug("No new releases found")
            #self.LastVersionData = ''
            return False
        # Assign next element as new data
        self.VersionData = VersionList[i+1]
        self.VersionNumber = self._getversioninfo(self.VersionData)[0]
        #self.VersionTime = self._getversioninfo(self.VersionData)[1]
        #strip list to current and new
        VersionList = VersionList[(i+1):]
        #while list size != 1
        while (len(VersionList) > 1):
            # scan through list
            for data in VersionList:
                # compare with current version
                dataNumber = self._getversioninfo(data)[0]
                if (dataNumber == self.VersionNumber):
                    # if so, strip list
                    VersionList = VersionList[1:]
                    break
            # assign list[0] to data
            self.VersionData = VersionList[0]
        self.VersionNumber = self._getversioninfo(self.VersionData)[0]
        self.VersionTime = self._getversioninfo(self.VersionData)[1]
        logger.info("Installing release %s created on %s" % (self.VersionNumber,self.VersionTime ))
        f.close()
        return True

    def _getversioninfo(self, VersionData):
        EqualsPoint = 0
        for i in range(len(VersionData)):
            if VersionData[i] == "-":
                EqualsPoint = i
        Number = VersionData[:(EqualsPoint-1)].strip()
        Date = VersionData[(EqualsPoint+1):].strip()
        try:
            Date = time.strptime(Date,"%d %b %Y %H:%M:%S")
        except ValueError as e:
           raise GangaRobotFatalError(" Incorrect format of file", ValueError)
        Date = datetime.datetime(*(Date[0:6]))
        Data = [Number, Date]
        return Data
                
    def _heartbeat(self):
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
        except IOError as e:
            f  = open(heartbeatfile, 'w')
            f.write(datetime.datetime.now().strftime("%H:%M:%S %j %y")+" - "+str(os.getpid()))
            f.close()
            
    def _emailer(self, subject, text):
        #Function to email list on startup of ganga (following restart by cronjob)
        from email.MIMEText import MIMEText
        from smtplib import SMTP
        emailcfg = Ganga.Utility.Config.getConfig('Robot')
        host = emailcfg['FileEmailer_Host']
        from_ = emailcfg['FileEmailer_From']
        recipients = [recepient.strip() for recepient in \
                        emailcfg['FileEmailer_Recipients'].split(',') \
                        if recepient.strip()]
        subject = "Ganga - TestRobot automatic restart by crontab"
        
        if not recipients:
            logger.warn('No recpients specified - email will not be sent')
            return
            
        logger.info("emailing files to %s." %(recipients))
        text = "GangaTestRobot restarted on: %s" %(datetime.datetime.now().strftime("%H:%M:%S %d %b %Y"))
        msg = MIMEText(text)
        msg['Subject'] = subject
        msg['From'] = from_
        msg['To'] = ', '.join(recipients)
        
        #send message
        session = SMTP()
        try:
            session.connect(host)
            session.sendmail(from_, recipients, msg.as_string())
            session.quit()
        except:
            logger.error("Failed to send notification of start-up")
        
        session.close()
            

    def __init__(self):
        self.InitialTime = datetime.datetime.fromtimestamp(0.0) #start of epoch
        #logger.info(self.LastCheckedTime)

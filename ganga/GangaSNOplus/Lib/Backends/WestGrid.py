import datetime
import time
from GangaCore.GPIDev.Adapters.IBackend import IBackend
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import *
from GangaCore.Core.exceptions import BackendError
from GangaCore.Lib.Batch import Batch

import GangaCore.Utility.Config

from GangaCore.Core import FileWorkspace
import os




config = GangaCore.Utility.Config.makeConfig('WestGrid','internal WestGrid command line interface')

config.addOption('shared_python_executable', False, "Shared PYTHON")

config.addOption('jobid_name', 'PBS_JOBID', "Name of environment with ID of the job")
config.addOption('queue_name', 'PBS_QUEUE', "Name of environment with queue name of the job")
config.addOption('heartbeat_frequency', '30', "Heartbeat frequency config variable")

config.addOption('submit_str', 'cd %s; qsub %s %s %s %s', "String used to submit job to queue")
config.addOption('submit_res_pattern', '^(?P<id>\S*)', "String pattern for replay from the submit command")

config.addOption('stdoutConfig', '-o %s/stdout', "String pattern for defining the stdout")
config.addOption('stderrConfig', '-e %s/stderr', "String pattern for defining the stderr")

config.addOption('kill_str', 'qdel %s', "String used to kill job")
config.addOption('kill_res_pattern', '(^$)|(qdel: Unknown Job Id)', "String pattern for replay from the kill command")

tempstr='''
env = os.environ
jobnumid = env["PBS_JOBID"]
os.system("mkdir /tmp/%s/" %jobnumid)
os.chdir("/tmp/%s/" %jobnumid)
os.environ["PATH"]+=":."
'''
config.addOption('preexecute', tempstr, "String contains commands executing before submiting job to queue")

tempstr='''
env = os.environ
jobnumid = env["PBS_JOBID"]
os.chdir("/tmp/")
os.system("rm -rf /tmp/%s/" %jobnumid) 
'''
config.addOption('postexecute', tempstr, "String contains commands executing before submiting job to queue")
config.addOption('jobnameopt', 'N', "String contains option name for name of job in batch system")
config.addOption('timeout',600,'Timeout in seconds after which a job is declared killed if it has not touched its heartbeat file. Heartbeat is touched every 30s so do not set this below 120 or so.')

config.addOption('voproxy',None,'Path to your vo-proxy')

class WestGrid(Batch):
    ''' WestGrid backend - submit jobs to Portable Batch System.
    '''
    #same schema as a Batch, but with the added option of where the user proxy is stored
    _schema = Schema(Version(1,0), {'queue' : SimpleItem(defvalue='',doc='queue name as defomed in your local Batch installation'),
                                    'extraopts' : SimpleItem(defvalue='',doc='extra options for Batch. See help(Batch) for more details'),
                                    'id' : SimpleItem(defvalue='',protected=1,copyable=0,doc='Batch id of the job'),
                                    'exitcode' : SimpleItem(defvalue=None,typelist=['int','type(None)'],protected=1,copyable=0,doc='Process exit code'),
                                    'status' : SimpleItem(defvalue='',protected=1,hidden=1,copyable=0,doc='Batch status of the job'),
                                    'actualqueue' : SimpleItem(defvalue='',protected=1,copyable=0,doc='queue name where the job was submitted.'),
                                    'actualCE' : SimpleItem(defvalue='',protected=1,copyable=0,doc='hostname where the job is/was running.'),
                                    'voproxy' : SimpleItem(defvalue=None,typelist=['str','type(None)'],doc='select specific proxy location')
                                    })
    _category = 'backends'
    _name = 'WestGrid'

    config = GangaCore.Utility.Config.getConfig('WestGrid')
    def __init__(self):
        super(WestGrid,self).__init__()
        if self.voproxy:
            print "USING MY OWN VO PROXY:",self.voproxy
        elif self.config['voproxy']:
            self.voproxy = os.path.expanduser(self.config['voproxy'])

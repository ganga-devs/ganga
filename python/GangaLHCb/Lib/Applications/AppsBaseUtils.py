#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import tempfile
#from PythonOptionsParser import PythonOptionsParser
#from Ganga.Core import ApplicationConfigurationError
from Ganga.Utility.Shell import Shell
import Ganga.Utility.logging
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler
import Ganga.Utility.Config

logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def backend_handlers():
  backends={'LSF'         : LHCbGaudiRunTimeHandler,
            'Interactive' : LHCbGaudiRunTimeHandler,
            'PBS'         : LHCbGaudiRunTimeHandler,
            'SGE'         : LHCbGaudiRunTimeHandler,
            'Local'       : LHCbGaudiRunTimeHandler,
            'Condor'      : LHCbGaudiRunTimeHandler,
            'Remote'      : LHCbGaudiRunTimeHandler,
            'Dirac'       : GaudiDiracRTHandler
            }
  return backends

def available_apps():
  return ["Gauss", "Boole", "Brunel", "DaVinci", "Moore", "Vetra",
          "Panoptes", "Erasmus"]

def available_packs(appname):
  packs={'Gauss'   : 'Sim',
         'Boole'   : 'Digi',
         'Brunel'  : 'Rec',
         'DaVinci' : 'Phys',
         'Moore'   : 'Hlt',
         'Vetra'   : 'Tell1',
         'Panoptes': 'Rich',
         'Bender'  : 'Phys',
         'Erasmus' : ''
         }
  return packs[appname]

def available_versions(appname):
  """Provide a list of the available Gaudi application versions"""
  
  s = Shell()
  tmp = tempfile.NamedTemporaryFile(suffix='.log')
  command = 'SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s >& %s; echo" % (command,tmp.name))
  output = tmp.read()
  tmp.close()
  versions = output[output.rfind('(')+1:output.rfind('q[uit]')].split()
  return versions

def guess_version(appname):
  """Guess the default Gaudi application version"""
  s = Shell()
  tmp = tempfile.NamedTemporaryFile(suffix='.log')
  command = 'SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s >& %s; echo" % (command,tmp.name))
  output = tmp.read()
  tmp.close()
  version = output[output.rfind('[')+1:output.rfind(']')]
  return version



#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

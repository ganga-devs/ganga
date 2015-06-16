#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os, sys
#from PythonOptionsParser import PythonOptionsParser
#from Ganga.Core import ApplicationConfigurationError
from Ganga.Utility.Shell                                   import Shell
from Ganga.Utility.logging                                 import getLogger
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler      import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler
import Ganga.Utility.Config

from GangaGaudi.Lib.Applications.Gaudi import Gaudi
from Ganga.GPIDev.Lib.Tasks.TaskApplication import taskify
from Ganga.GPIDev.Lib.Tasks.TaskApplication import task_map

from Ganga.GPIDev.Schema import SimpleItem

logger = getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def backend_handlers():
  backends={'LSF'         : LHCbGaudiRunTimeHandler,
            'Interactive' : LHCbGaudiRunTimeHandler,
            'PBS'         : LHCbGaudiRunTimeHandler,
            'SGE'         : LHCbGaudiRunTimeHandler,
            'Local'       : LHCbGaudiRunTimeHandler,
            'Condor'      : LHCbGaudiRunTimeHandler,
            'Remote'      : LHCbGaudiRunTimeHandler,
            'Dirac'       : LHCbGaudiDiracRunTimeHandler
            }
  return backends

available_lhcb_apps = ["Gauss", "Boole", "Brunel",
                       "DaVinci", "Moore", "Vetra",
                       "Panoptes", "Erasmus", "Alignment",
                       "Noether", "Urania" ]

def available_apps():
    return available_lhcb_apps

available_lhcb_packs={
     'Gauss'     : 'Sim',
     'Boole'     : 'Digi',
     'Brunel'    : 'Rec',
     'DaVinci'   : 'Phys',
     'Moore'     : 'Hlt',
     'Vetra'     : 'Tell1',
     'Panoptes'  : 'Rich',
     'Bender'    : 'Phys',
     'Erasmus'   : '',
     'Noether'   : '',
     'Urania'    : 'PID',
     'Alignment' : 'Alignment/Escher'
     }

def available_packs(appname):
    return available_lhcb_packs[appname]

def addNewLHCbapp( appname, use='' ):
    assert isinstance( appname, str )
    if any(str(appname).lower() == val.lower() for val in available_lhcb_apps ):
        logger.warning( "Error: %s is already in the list of supported apps, not adding" % appname )
        return
    available_lhcb_apps.append( str(appname) )
    available_lhcb_packs[ str(appname)] = use
    return

def available_versions(appname):
  """Provide a list of the available Gaudi application versions"""
  import EnvironFunctions
  return EnvironFunctions.available_versions( appname )

def guess_version(appname):
  """Guess the default Gaudi application version"""
  import EnvironFunctions
  return EnvironFunctions.guess_version( appname )


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def lumi(xmlsummary):
  '''given an XMLSummary object, will return the integrated luminosity'''
  #  print xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].value()[0],'+/-',xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].value()[2]

  lumiDict = dict( zip( xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].attrib('format'),
                        xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].value()
                        )
                   )
  return '"%s +- %s"' % (lumiDict['Flag'], lumiDict['Flag2'])

def events(xmlsummary):
  '''given an XMLSummary object, will return the number of events input/output'''
  ad=xmlsummary.file_dict()
  evts={}
  for type in ad.keys():
    if type not in evts:
      evts[type]=0
    for file in ad[type].keys():
      if type=='input' and ad[type][file].attrib('status')=='mult':
        logger.warning('Warning, processed file ' + ad[type][file].attrib('name') + 'multiple times')
      if ad[type][file].attrib('GUID')==file:
        continue
      else:
        evts[type]+=ad[type][file].value()
  return evts

def xmldatafiles(xmlsummary):
  '''return a dictionary of the files the xmlsummary saw as input'''
  returndict={}
  for file in xmlsummary.file_dict()['input'].values():
    if file.attrib('status') in returndict:
      returndict[file.attrib('status')].update([file.attrib('name')])
    else:
      returndict[file.attrib('status')]=set([file.attrib('name')])
  return returndict

def xmldatanumbers(xmlsummary):
  '''return a dictionary of the number of files the xmlsummary saw as input'''
  returndict={}
  for file in xmlsummary.file_dict()['input'].values():
    if file.attrib('status') in returndict:
      returndict[file.attrib('status')]=returndict[file.attrib('status')]+1
    else:
      returndict[file.attrib('status')]=1
  return returndict

def xmlskippedfiles(xmlsummary):
  '''get all skipped files from xml'''
  filedict=xmldatafiles(xmlsummary)
  skippedfiles=set()
  for stat in ['none','fail']:
    if stat in filedict:
      skippedfiles.update(filedict[stat])
  return skippedfiles

def activeSummaryItems():
  activeItems = {'lumi'           :lumi,
                 'events'         :events,
                 'xmldatafiles'   :xmldatafiles,
                 'xmldatanumbers' :xmldatanumbers,
                 'xmlskippedfiles':xmlskippedfiles
                 }
  return activeItems

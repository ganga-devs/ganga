#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import tempfile,os, sys
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
          "Panoptes", "Erasmus","Alignment"]

def available_packs(appname):
  packs={'Gauss'     : 'Sim',
         'Boole'     : 'Digi',
         'Brunel'    : 'Rec',
         'DaVinci'   : 'Phys',
         'Moore'     : 'Hlt',
         'Vetra'     : 'Tell1',
         'Panoptes'  : 'Rich',
         'Bender'    : 'Phys',
         'Erasmus'   : '',
         'Alignment' : 'Alignment/Escher'
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
        print 'Warning, processed file ', ad[type][file].attrib('name'), 'multiple times'
      if ad[type][file].attrib('GUID')==file:
        #print 'ignoring'
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
  
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def app_postprocess(job):
  parsedXML = os.path.join(job.outputdir,'__parsedxmlsummary__')
  metadataItems={} # use to avoid replacing 'lumi' etc as return value and not the method pointer
  if os.path.exists(parsedXML):
    execfile(parsedXML,{},metadataItems)

  # Combining subjobs XMLSummaries.
  if job.subjobs:
    env = job.application.getenv()
    if 'XMLSUMMARYBASEROOT' not in env:
      logger.error('"XMLSUMMARYBASEROOT" env var not defined so summary.xml files not merged for subjobs of job %s' % job.fqid)
      return

    summaries = []
    for sj in job.subjobs:
      outputxml = os.path.join(sj.outputdir,'summary.xml')
      if not os.path.exists(outputxml):
        logger.warning("XMLSummary for job %s will not be merged as 'summary.xml' not present in job %s outputdir" % (sj.fqid,sj.fqid))
        continue
      summaries.append(outputxml)

    if not summaries:
      logger.error('None of the subjobs of job %s produced the output XML summary file "summary.xml". Merging will therefore not happen' % job.fqid)
      return
            
    schemapath  = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'xml/XMLSummary.xsd')
    summarypath = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'python/XMLSummaryBase')
    sys.path.append(summarypath)
    import summary

    try:
      XMLSummarydata = summary.Merge(summaries,schemapath)
    except:
      logger.error('Problem while merging the subjobs XML summaries')
      raise

    for name, method in activeSummaryItems().iteritems():
      try:
        metadataItems[name] = method(XMLSummarydata)
      except:
        metadataItems[name] = None
        logger.warning('Problem running "%s" method on merged xml output.' % name)

  for key, value in metadataItems.iteritems():
    if value is None: # Has to be explicit else empty list counts
      job.metadata[key] = 'Not Available.'
    else:
      job.metadata[key] = value

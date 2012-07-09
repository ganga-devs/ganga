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
  return (xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].value())

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
#  from Ganga.GPI import LHCbDataset
  skippedfiles=set()
  for stat in ['none','fail']:
    if stat in filedict:
      skippedfiles.update(filedict[stat])
  return skippedfiles

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def app_postprocess(job):
  parsedXML = os.path.join(job.outputdir,'__parsedxmlsummary__')
  if os.path.exists(parsedXML):
    with open(parsedXML,'r') as f:
      for l in f.readlines():
        key, value = l[:-1].split('->')#-1 removes the \n newline char
        job.metadata[key] = value

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

    try:
      l=lumi(XMLSummarydata)
      job.metadata['lumi']="%s +- %s, from %s files" % (l[0],l[2],l[1])
    except:
      logger.warning('Problem calculating the merged luminosity.')
      job.metadata['lumi']="NotAvailable"

    try:
      job.metadata['events' ]=str(events(XMLSummarydata))
    except:
      logger.warning('Problem calculating the merged number of events IN/OUT.')
      job.metadata['events']="NotAvailable"
                
    try:
      job.metadata['xmldatafiles'] = str(xmldatafiles(XMLSummarydata))
    except:
      logger.warning('Failed to compute merged set of datafiles run over from xml.')
      job.metadata['xmldatafiles']="NotAvailable"
          
    try:
      job.metadata['xmldatanumbers'] = str(xmldatanumbers(XMLSummarydata))
    except:
      logger.warning('Failed to compute No. of datafiles run over from merger xml.')
      job.metadata['xmldatanumbers'] = "NotAvailable"

    try:
      job.metadata['xmlskippedfiles']= str(xmlskippedfiles(XMLSummarydata))
    except:
      logger.warning('Failed to compute No. of datafiles skipped from merged xml.')
      job.metadata['xmlskippedfiles']="NotAvailable"

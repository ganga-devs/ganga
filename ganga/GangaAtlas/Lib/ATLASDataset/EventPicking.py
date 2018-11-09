##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: $
###############################################################################
# A DQ2 dataset superclass, with event picking capability

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Schema import *
from GangaCore.Utility.logging import getLogger
import os

logger = getLogger()


class EventPicking(DQ2Dataset):
    '''Event picking with DQ2 Dataset support '''

    _category = 'datasets'
    _name = 'EventPicking'

    _schema = DQ2Dataset._schema.inherit_copy()
    _schema.datadict['pick_data_type'] = SimpleItem(defvalue = '', doc = 'Type of data for event picking. One of AOD, ESD, RAW.')
    _schema.datadict['pick_stream_name'] = SimpleItem(defvalue = '', doc = 'Stream name for event picking, e.g. physics_L1Calo')
    _schema.datadict['pick_dataset_pattern'] = SimpleItem(defvalue = '', doc="Dataset pattern which matches the selection " )
    _schema.datadict['pick_event_list'] = FileItem(doc = 'A filename which contains list of runs/events for event picking.')
    _schema.datadict['event_pick_amitag'] = SimpleItem(defvalue = '', doc = 'AMI tag used to match TAG collections names. This option is required when you are interested in older data than the latest one. Either \ or "" is required when a wild-card is used. e.g., f2\*.')
    _schema.datadict['pick_filter_policy'] = SimpleItem(defvalue = 'accept', doc = 'accept/reject the pick event.')

    _exportmethods = ['get_pick_dataset']

    def __init__(self):
        super( EventPicking, self ).__init__()

    # get list of datasets and files by list of runs/events
    def get_pick_dataset(self, verbose=False):
        
        job = self._getParent()
        if job and job.inputdata and job.inputdata.pick_event_list.name != '' and  len(job.inputdata.dataset) !=0 :
            raise ApplicationConfigurationError('Cannot use event pick list and input dataset at the same time.')

        #parametr check for event picking
        if job and job.inputdata and job.inputdata.pick_event_list.name != '':
            if job.inputdata.pick_data_type == '':
                raise ApplicationConfigurationError('Event pick data type (pick_data_type) must be specified.')

        # set X509_USER_PROXY
        from pandatools import Client
        if 'X509_USER_PROXY' not in os.environ or os.environ['X509_USER_PROXY'] == '':
            os.environ['X509_USER_PROXY'] = Client._x509()

        # setup eventLookup
        from pandatools.eventLookupClient import eventLookupClient
        elssiIF = eventLookupClient()
        # open run/event txt
        runEvtList = []
        if os.path.exists( self.pick_event_list.name ):
            logger.info("Event pick list file %s selected" % self.pick_event_list.name)
            runevttxt = open(self.pick_event_list.name)
            for line in runevttxt:
                items = line.split()
                if len(items) != 2:
                    continue
                runNr,evtNr = items
                runEvtList.append([runNr,evtNr])
                # close
            runevttxt.close()

        else:
            raise ApplicationConfigurationError('Could not read event pick list file %s.' %self.pick_event_list.name)

        # convert self.pick_data_type to Athena stream ref
        if self.pick_data_type == 'AOD':
            streamRef = 'StreamAOD_ref'
        elif self.pick_data_type == 'ESD':
            streamRef = 'StreamESD_ref'
        elif self.pick_data_type == 'RAW':
            streamRef = 'StreamRAW_ref'
        else:
            errStr  = 'invalid data type %s for event picking. ' % self.pick_data_type
            errStr += ' Must be one of AOD,ESD,RAW'
            raise ApplicationConfigurationError(errStr)
        logger.info('Getting dataset names and LFNs from ELSSI for event picking')

        # read
        guids = []
        guidRunEvtMap = {}
        runEvtGuidMap = {}
        # bulk lookup
        nEventsPerLoop = 500
        iEventsTotal = 0
        while iEventsTotal < len(runEvtList):
            tmpRunEvtList = runEvtList[iEventsTotal:iEventsTotal+nEventsPerLoop]
            iEventsTotal += nEventsPerLoop
            paramStr = 'Run, Evt: %s, Stream: %s, Dataset pattern: %s' % (tmpRunEvtList,self.pick_stream_name, self.pick_dataset_pattern)
            logger.debug(paramStr)
            logger.info('.')
            # check with ELSSI
            if self.pick_stream_name == '':
                guidListELSSI = elssiIF.doLookup(tmpRunEvtList,tokens=streamRef,amitag=self.event_pick_amitag,extract=True)
            else:
                guidListELSSI = elssiIF.doLookup(tmpRunEvtList,stream=self.pick_stream_name,tokens=streamRef,amitag=self.event_pick_amitag,extract=True)

            if len(guidListELSSI) == 0 or guidListELSSI is None:
                errStr = ''
                for tmpLine in elssiIF.output:
                    errStr += tmpLine + '\n'
                errStr = "GUID was not found in ELSSI.\n" + errStr
                raise ApplicationConfigurationError(errStr)

            # check attribute
            attrNames, attrVals = guidListELSSI
            def getAttributeIndex(attr):
                for tmpIdx,tmpAttrName in enumerate(attrNames):
                    if tmpAttrName.strip() == attr:
                        return tmpIdx
                errStr = "cannot find attribute=%s in %s provided by ELSSI" % (attr,str(attrNames))
                raise ApplicationConfigurationError(errStr)
            # get index
            indexEvt = getAttributeIndex('EventNumber')
            indexRun = getAttributeIndex('RunNumber')
            indexTag = getAttributeIndex(streamRef)
            # check events
            for runNr,evtNr in tmpRunEvtList:
                paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,self.pick_stream_name)
                # collect GUIDs
                tmpguids = []
                for attrVal in attrVals:
                    if runNr == attrVal[indexRun] and evtNr == attrVal[indexEvt]:
                        tmpGuid = attrVal[indexTag]
                        # check non existing
                        if tmpGuid == 'NOATTRIB':
                            continue
                        if not tmpGuid in tmpguids:
                            tmpguids.append(tmpGuid)
                # not found
                if tmpguids == []:
                    errStr = "no GUIDs were found in ELSSI for %s" % paramStr
                    raise ApplicationConfigurationError(errStr)
                # append
                for tmpguid in tmpguids:
                    if not tmpguid in guids:
                        guids.append(tmpguid)
                        guidRunEvtMap[tmpguid] = []
                    guidRunEvtMap[tmpguid].append((runNr,evtNr))
                runEvtGuidMap[(runNr,evtNr)] = tmpguids

        # convert to dataset names and LFNs
        dsLFNs,allDSMap = Client.listDatasetsByGUIDs(guids,self.pick_dataset_pattern,verbose)
        logger.debug(dsLFNs)
        
        #populate DQ2Datase    
        if job and job.inputdata:
            job.inputdata.dataset = []
            job.inputdata.names = []
            job.inputdata.guids = []

            # check duplication
            for runNr,evtNr in runEvtGuidMap.keys():
                tmpLFNs = []
                tmpAllDSs = {}
                for tmpguid in runEvtGuidMap[(runNr,evtNr)]:
                    if tmpguid in dsLFNs:
                        tmpLFNs.append(dsLFNs[tmpguid])
                        job.inputdata.guids.append(tmpguid)
                        job.inputdata.names.append(dsLFNs[tmpguid][1])
                        if not ((dsLFNs[tmpguid][0]) in job.inputdata.dataset):
                            job.inputdata.dataset.append(dsLFNs[tmpguid][0])
                    else:
                        tmpAllDSs[tmpguid] = allDSMap[tmpguid]
                        if tmpguid in guidRunEvtMap:
                            del guidRunEvtMap[tmpguid]
                # empty        
                if tmpLFNs == []:
                    paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,self.pick_stream_name)                        
                    errStr = "Dataset pattern '%s' didn't pick up a file for %s\n" % (self.pick_dataset_pattern,paramStr)
                    for tmpguid,tmpAllDS in tmpAllDSs.iteritems():
                        errStr += "    GUID:%s dataset:%s\n" % (tmpguid,str(tmpAllDS))
                    raise ApplicationConfigurationError(errStr)
                # duplicated    
                if len(tmpLFNs) != 1:
                    paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,self.pick_stream_name)            
                    errStr = "Multiple LFNs %s were found in ELSSI for %s. Please set pick_dataset_pattern and/or pick_stream_name and or event_pick_amitag correctly." %(str(tmpLFNs),paramStr)
                    raise ApplicationConfigurationError(errStr)

        
        # return
        return guidRunEvtMap

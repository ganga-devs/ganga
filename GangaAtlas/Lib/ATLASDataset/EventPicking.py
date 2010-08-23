##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: $
###############################################################################
# A DQ2 dataset superclass, with event picking capability

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Schema import *
from Ganga.Utility.logging import getLogger
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
    _schema.datadict['pick_filter_policy'] = SimpleItem(defvalue = 'accept', doc = 'accept/reject the pick event.')

    _exportmethods = ['get_pick_dataset']

    def __init__(self):
        super( EventPicking, self ).__init__()

    # get list of datasets and files by list of runs/events
    def get_pick_dataset(self, verbose=False):
        
        job = self._getParent()
        if job.inputdata.pick_event_list.name != '' and  len(job.inputdata.dataset) !=0 :
            raise ApplicationConfigurationError(None, 'Cannot use event pick list and input dataset at the same time.')

        #parametr check for event picking
        if job.inputdata.pick_event_list.name != '':
            if job.inputdata.pick_data_type == '':
                raise ApplicationConfigurationError(None, 'Event pick data type (pick_data_type) must be specified.')

        from pandatools import eventLookup, Client
        elssiIF = eventLookup.pyELSSI()
        # set X509_USER_PROXY
        if not os.environ.has_key('X509_USER_PROXY') or os.environ['X509_USER_PROXY'] == '':
            os.environ['X509_USER_PROXY'] = Client._x509()
        # open run/event txt
        if os.path.exists( self.pick_event_list.name ):
            logger.info("Event pick list file %s selected" % self.pick_event_list.name)
            runevttxt = open(self.pick_event_list.name)
        else:
            raise ApplicationConfigurationError(None, 'Could not read event pick list file %s.' %self.pick_event_list.name)

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
            raise ApplicationConfigurationError(None,errStr)
        logger.info('Getting dataset names and LFNs from ELSSI for event picking')

        # read
        runEvtList = []
        guids = []
        guidRunEvtMap = {}
        runEvtGuidMap = {}    
        for line in runevttxt:
            items = line.split()
            if len(items) != 2:
                continue
            runNr,evtNr = items
            paramStr = 'Run:%s Evt:%s Stream:%s Dataset pattern %s' % (runNr,evtNr,self.pick_stream_name, self.pick_dataset_pattern)
            logger.info(paramStr)
            # check with ELSSI
            if self.pick_stream_name == '':
                guidListELSSI = elssiIF.eventLookup(runNr,evtNr,[streamRef],verbose=verbose)
            else:
                guidListELSSI = elssiIF.eventLookup(runNr,evtNr,[streamRef],self.pick_stream_name,verbose=verbose)            
            if guidListELSSI == []:
                errStr = "GUID was not found in ELSSI for %s" % paramStr    
                raise ApplicationConfigurationError(None,errStr)
            # check duplication
            tmpguids = []
            for tmpGuid, in guidListELSSI:
                if tmpGuid == 'NOATTRIB':
                    continue
                if not tmpGuid in tmpguids:
                    tmpguids.append(tmpGuid)
            if tmpguids == []:
                errStr = "no GUIDs were found in ELSSI for %s" % paramStr
                raise ApplicationConfigurationError(None,errStr)
            # append
            for tmpguid in tmpguids:
                if not tmpguid in guids:
                    guids.append(tmpguid)
                    guidRunEvtMap[tmpguid] = []
                guidRunEvtMap[tmpguid].append((runNr,evtNr))
            runEvtGuidMap[(runNr,evtNr)] = tmpguids
        # close
        runevttxt.close()
        # convert to dataset names and LFNs
        dsLFNs,allDSMap = Client.listDatasetsByGUIDs(guids,self.pick_dataset_pattern,verbose)
        logger.debug(dsLFNs)
        
        #populate DQ2Dataset    
        job.inputdata.dataset = []
        job.inputdata.names = []
        job.inputdata.guids = []

        # check duplication
        for runNr,evtNr in runEvtGuidMap.keys():
            tmpLFNs = []
            tmpAllDSs = {}
            for tmpguid in runEvtGuidMap[(runNr,evtNr)]:
                if dsLFNs.has_key(tmpguid):
                    tmpLFNs.append(dsLFNs[tmpguid])
                    job.inputdata.guids.append(tmpguid)
                    job.inputdata.names.append(dsLFNs[tmpguid][1])
                    if not ((dsLFNs[tmpguid][0]) in job.inputdata.dataset):
                        job.inputdata.dataset.append(dsLFNs[tmpguid][0])
                else:
                    tmpAllDSs[tmpguid] = allDSMap[tmpguid]
                    if guidRunEvtMap.has_key(tmpguid):
                        del guidRunEvtMap[tmpguid]
            # empty        
            if tmpLFNs == []:
                paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,self.pick_stream_name)                        
                errStr = "Dataset pattern '%s' didn't pick up a file for %s\n" % (self.pick_dataset_pattern,paramStr)
                for tmpguid,tmpAllDS in tmpAllDSs.iteritems():
                    errStr += "    GUID:%s dataset:%s\n" % (tmpguid,str(tmpAllDS))
                raise ApplicationConfigurationError(None,errStr)
            # duplicated    
            if len(tmpLFNs) != 1:
                paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,self.pick_stream_name)            
                errStr = "Multiple LFNs %s were found in ELSSI for %s. Please set pick_dataset_pattern and/or pick_stream_name correctly" %(str(tmpLFNs),paramStr)
                raise ApplicationConfigurationError(None,errStr)

        
        # return
        return guidRunEvtMap

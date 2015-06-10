##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: $
###############################################################################
# A DQ2 dataset superclass, with AMI connection and search capability

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, listDatasets
from Ganga.GPIDev.Schema.Schema import SimpleItem
from Ganga.GPIDev.Schema.Schema import FileItem
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig, ConfigError

import os

logger = getLogger()
config = getConfig('AMIDataset')

try:
    from pyAMI.pyAMI import AMI as AMIClient
    from pyAMI.pyAMIEndPoint import pyAMIEndPoint

    #Reading from CERN replica
    pyAMIEndPoint.setType("replica")
    amiclient = AMIClient()

except ImportError:
    logger.warning("AMI not properly set up. Set athena to access AMI from this ganga session.")
    pass

def resolve_dataset_name(name = ''):
    result = listDatasets(name + '*')
    status = False
    for dset in result:
        if dset[0] == name + '/':
            status = True
    return status

def get_metadata(dataset = '', file_name = ''):
    
    argument = []
    argument.append("GetDatasetInfo")
    if dataset:
        argument.append("logicalDatasetName=" + dataset)
        limit="0,%d" %config['MaxNumOfDatasets']
    else:
        argument.append("logicalFileName=" + file_name)
        limit="0,%d" %config['MaxNumOfFiles']

    argument.append("limit=" + limit)
    #Dictionary which contain all metadata info about a dataset or file
    metadata = {}       

    try:
        result =  amiclient.execute(argument)
        resultDict = result.getDict()
        resultByRow = resultDict['Element_Info']
        for rows, ids in resultByRow.iteritems():
            for metaid, id in ids.iteritems():
                metadata[str(metaid)] = str(id)

    except Exception as msg:
        logger.warning("Couldn't get metadata from AMI due to %s" % msg)

    return metadata     


def get_file_metadata(dataset='', all=False, numevtsperfile = 0):
    
    argument = []
    argument.append("ListFiles")
    argument.append("logicalDatasetName=" + dataset)
    limit="0,%d" %config['MaxNumOfFiles']
    argument.append("limit=" + limit)
    
    #Metatdata from all the files from a dataset
    info = []

    try:
        result =  amiclient.execute(argument)
        resultDict = result.getDict()
        resultByRow = resultDict['Element_Info']
        for rows, ids in resultByRow.iteritems():
            if all:
                file_metadata = get_metadata(file_name = ids['LFN'])
            else:
                file_metadata = {}
            for id,val in ids.iteritems():
                attr = str(id).lower()
                if ( (attr == 'filesize' or attr == 'events') and len(val) > 0):
                    val = int(val)
                file_metadata[attr]  = val 
            
            nevents = file_metadata.setdefault('events', 0)
            info.append(file_metadata)
    
    except Exception as msg:
        logger.warning("Couldn't get file metadata from AMI due to %s" % msg)
    
    content = {}
    for i in info:
        guid = str(i.pop('fileguid'))
        content[guid] = i
    
    return content 


class AMIDataset(DQ2Dataset):
    '''ATLAS DDM Dataset With AMI Connection'''

    _category = 'datasets'
    _name = 'AMIDataset'
            
    _schema = DQ2Dataset._schema.inherit_copy()
    _schema.datadict['logicalDatasetName'] = SimpleItem(defvalue = '', doc = '')
    _schema.datadict['project'] = SimpleItem(defvalue = 'Atlas_Production', doc = '')
    _schema.datadict['processingStep'] = SimpleItem(defvalue = 'Atlas_Production', doc = '')
    _schema.datadict['amiStatus'] = SimpleItem(defvalue = 'VALID', doc = '')
    _schema.datadict['entity'] = SimpleItem(defvalue = 'dataset', doc = '')
    _schema.datadict['metadata'] = SimpleItem(defvalue = {}, doc="Metadata" )
    _schema.datadict['provenance'] = SimpleItem(defvalue=[], typelist=['str'], sequence=1, doc='Dataset provenance chain')
    _schema.datadict['goodRunListXML'] = FileItem(doc = 'GoodRunList XML file to search on')


    _exportmethods = ['search','fill_provenance', 'get_datasets_metadata', 'get_files_metadata', 'get_contents']


    def __init__(self):
        super( AMIDataset, self ).__init__()

    def fill_provenance(self,extraargs = []):

        dataType=""
        if(len(extraargs)>1 ):
            dataType= extraargs[1]
                                
        self.provenance = []

        for d in self.dataset:

            logger.info("Filling provenance info for dataset %s...", d )

            prov = []
            self.provenance.append(prov)
            
            ds = d[:-1]

            argument=[]
            argument.append("ListDatasetProvenance")
            argument.append("logicalDatasetName="+ds)
            argument.append('-output=xml')

            result= amiclient.execute(argument)

            dom = result.getAMIdom()
            graph = dom.getElementsByTagName('graph')
            nFound = 0
            dictOfLists={}
            for line in graph:
                nodes = line.getElementsByTagName('node')
                for node in nodes:
                    level=int(node.attributes['level'].value)
                    dataset = node.attributes['name'].value
                    if (len(dataType)>0)and(dataset.find(dataType)>=0):
                        # print only selected dataType
                        levelList=dictOfLists.get(level,[])
                        levelList.append(dataset)
                        dictOfLists[level] = levelList 
                        nFound=nFound+1
                    elif (len(dataType)== 0):
                        #print everything 
                        levelList=dictOfLists.get(level,[])
                        levelList.append(dataset)    
                        dictOfLists[level] = levelList
                        #print level,dictOfLists[level]
                        nFound=nFound+1
            if(nFound==0)and (len(dataType)>0):
                logger.warning( "No datasets found of type",dataType)
            else:
                keys = dictOfLists.keys()
            
                keys.sort()

                for key in keys:
                    datasetList=dictOfLists.get(key)
                    datasetList.sort()                    
                    #print "generation =",key
                    #for dataset in datasetList:
                    #    print " ",dataset 
                    for dataset in datasetList:
                        prov.append("%s/" % dataset.strip())

    def search(self, pattern='', maxresults = config['MaxNumOfDatasets'], extraargs = []):
        
        argument=[]
        dsetList = []
        
        if self.goodRunListXML.name != '':

            # open the GRL
            if os.path.exists( self.goodRunListXML.name ):
                logger.info("Good Run List '%s' file selected" % self.goodRunListXML.name)
                grl_text = open( self.goodRunListXML.name ).read()
            else:
                logger.error('Could not read Good Run List XML file')
                return []
            
            argument=[]
            argument.append("GetGoodDatasetList")
            argument.append("prodStep=merge")
            #argument.append("dataType=%s" % self.dataType)
            argument.append("goodRunList=%s" % grl_text)
            argument.append("logicalDatasetName=%s" % self.logicalDatasetName)
                   
        elif self.logicalDatasetName:
            pattern=self.logicalDatasetName
            
            pattern = pattern.replace("/","")    
            pattern = pattern.replace('*','%')
            limit="0,%d" %config['MaxNumOfDatasets']

            argument.append("SearchQuery")
            argument.append("entity=" + self.entity)

            argument.append("glite=SELECT logicalDatasetName WHERE amiStatus='" + self.amiStatus +"' AND logicalDatasetName like '" + pattern +"' LIMIT "+limit)

            argument.append("project=" + self.project)
            argument.append("processingStep=" + self.processingStep)
            argument.append("mode=defaultField")
            argument.extend(extraargs)

        else:
            logger.error("AMI search not set up correctly. No datasetname or good runs list supplied.")
            return []
        
        try:
            result = amiclient.execute(argument)
            if argument[0] == "GetGoodDatasetList":
                # GRL has different output
                res_text = result.output()
                dsetList = []
                for ln in res_text.split('\n'):
                    if ln.find("logicalDatasetName") != -1:
                        # add to the dataset list - check container...
                        if resolve_dataset_name(ln.split('=')[1].strip()):
                                dsetList.append(ln.split('=')[1].strip() + "/")
                        else:
                                dsetList.append(ln.split('=')[1].strip())
                                
            else:
                resultDict= result.getDict()
                resultByRow = resultDict['Element_Info']
                for row, vals in resultByRow.iteritems():
                    dsName = str(vals['logicalDatasetName'])
                    # check with DQ2 since AMI doesn't store /
                    if resolve_dataset_name(dsName):
                        dsName += '/'
                    dsetList.append(dsName)
                    
        except Exception as msg:
            logger.error( msg )

        return dsetList

    def get_datasets_metadata(self):
        datasets = self.search()
        metadata = []

        for dataset in datasets:
            dataset = dataset.replace("/","")    
            tmp = get_metadata(dataset = dataset)
            metadata.append(tmp)

        return metadata
    
    def get_files_metadata(self, all=False):
        datasets = self.search()
        metadata = {}

        for dataset in datasets:
            dataset = dataset.replace("/","")    
            job = self._getParent()
            file_info = get_file_metadata(dataset=dataset, all=all, numevtsperfile = 0)
            metadata.update(file_info)

        return metadata

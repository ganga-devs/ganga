##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: $
###############################################################################
# A DQ2 dataset superclass, with AMI connection and search capability

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset
from Ganga.GPIDev.Schema.Schema import SimpleItem
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError

logger = getLogger()
config = makeConfig('AMIDataset','AMI dataset')
config.addOption('MaxNumOfFiles', 100, 'Maximum number of files in a given dataset patterns')
config.addOption('MaxNumOfDatasets', 20, 'Maximum number of datasets in a given dataset patterns')

try:
    from pyAMI.pyAMI import AMI as AMIClient
except ImportError:
    logger.warning("AMI not properly set up. You will not be able to access AMI from this ganga session.")
    pass

def get_metadata(dataset = '', file_name = ''):
    
    try: 
        amiclient = AMIClient()
    except:
        logger.warning("Couldn't instantiate AMI client.  AMI not set up ?")

    argument = []
    argument.append("GetDatasetInfo")
    if dataset:
        argument.append("logicalDatasetName=" + dataset)
    else:
        argument.append("logicalFileName=" + file_name)

    #Dictionary which contain all metadata info about a dataset or file
    metadata = {}       

    try:
        result =  amiclient.execute(argument)
        resultDict = result.getDict()
        resultByRow = resultDict['Element_Info']
        for rows, ids in resultByRow.iteritems():
            for metaid, id in ids.iteritems():
                metadata[str(metaid)] = str(id)

    except Exception, msg:
        logger.warning("Couldn't get metadata from AMI due to %s" % msg)

    return metadata     


def get_file_metadata(dataset='', all=False):
    
    try: 
        amiclient = AMIClient()
    except:
        logger.warning("Couldn't instantiate AMI client.  AMI not set up ?")

    argument = []
    argument.append("ListFiles")
    argument.append("logicalDatasetName=" + dataset)
    
    #Metatdata from all the files from a datset
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
                file_metadata[id]  = val 
            
            info.append(file_metadata)
    
    except Exception, msg:
        logger.warning("Couldn't get metadata from AMI due to %s" % msg)

    return info 




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
    _schema.datadict['amiclient'] = SimpleItem(defvalue = 0, transient=1, hidden=1, doc="AMI client" )
    _schema.datadict['metadata'] = SimpleItem(defvalue = {}, doc="Metadata" )
    _schema.datadict['provenance'] = SimpleItem(defvalue=[], typelist=['str'], sequence=1, doc='Dataset provenance chain')


    _exportmethods = ['search','fill_provenance', 'get_datasets_metadata', 'get_files_metadata']


    def __init__(self):
        super( AMIDataset, self ).__init__()
        self._initami()

    def _initami(self):
        try:
            self.amiclient = AMIClient()
        except:
            logger.warning("Coldn't instantiate AMI client. AMI not set up?")
    
    def fill_provenance(self,extraargs = []):

        if not self.amiclient:
            self._initami()

        dataType=""
        if(len(extraargs)>1 ):
            dataType= extraargs[1]
                                
        self.provenance = []

        for d in self.dataset:

            print "Filling provenance info for dataset %s..." % d

            prov = []
            self.provenance.append(prov)
            
            ds = d[:-1]

            argument=[]
            argument.append("ListDatasetProvenance")
            argument.append("logicalDatasetName="+ds)
            argument.append('-output=xml')

            result= self.amiclient.execute(argument)

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
                print "No datasets found of type",dataType
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

    def search(self, pattern='', maxresults = 2, extraargs = []):
        
        if not self.amiclient:
            self._initami()

        argument=[]
        dsetList = []
        
        if self.goodRunListXML.name != '':

            # open the GRL
            if os.path.exists( self.goodRunListXML.name ):
                logger.warning("Good Run List '%s' file selected" % self.goodRunListXML.name)
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
            limit="0,%d" % maxresults

            argument.append("SearchQuery")
            argument.append("entity=" + self.entity)

            argument.append("glite=SELECT logicalDatasetName WHERE amiStatus='" + self.amiStatus +"' AND logicalDatasetName like '" + pattern +"' LIMIT "+limit)

            argument.append("project=" + self.project)
            argument.append("processingStep=" + self.processingStep)
            argument.append("mode=defaultField")
            argument.extend(extraargs)
        
        try:
            result = self.amiclient.execute(argument)
            resultDict= result.getDict()
            resultByRow = resultDict['Element_Info']
            for row, vals in resultByRow.iteritems():
                dsetList.append(str(vals['logicalDatasetName']))
        except Exception, msg:
            print msg

        return dsetList

    def get_datasets_metadata(self):
        datasets = self.search()
        metadata = []

        for dataset in datasets:
            tmp = get_metadata(dataset = dataset)
            metadata.append(tmp)

        return metadata
    
    def get_files_metadata(self, all=False):
        datasets = self.search()
        metadata = []

        for dataset in datasets:
            tmp = get_file_metadata(dataset=dataset, all=all)
            metadata =  metadata + tmp

        return metadata

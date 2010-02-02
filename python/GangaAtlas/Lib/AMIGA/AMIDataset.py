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
try:
    from pyAMI.pyAMI import AMI as AMIClient
except ImportError:
    logger.warning("AMI not properly set up. You will not be able to access AMI from this ganga session.")
    pass


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
    _schema.datadict['amiclient'] = SimpleItem(defvalue = 0, hidden=1, doc="AMI client" )
    _schema.datadict['metadata'] = SimpleItem(defvalue = {}, doc="Metadata" )
    _schema.datadict['provenance'] = SimpleItem(defvalue=[], typelist=['str'], sequence=1, doc='Dataset provenance chain')


    _exportmethods = ['search','fill_provenance']


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

    def search(self, pattern='', maxresults = 2,extraargs = []):
        
        if not self.amiclient:
            self._initami()

        if pattern=='':
            pattern=self.dataset[0]

        
        pattern = pattern.replace("/","")    
        pattern = pattern.replace('*','%')
            
        limit="0,%d" % maxresults

        argument=[]
        argument.append("SearchQuery")
        argument.append("entity=" + self.entity)
        
        argument.append("glite=SELECT logicalDatasetName WHERE amiStatus='" + self.amiStatus +"' AND logicalDatasetName like '" + pattern +"' LIMIT "+limit)
        
        argument.append("project=" + self.project)
        argument.append("processingStep=" + self.processingStep)
        argument.append("mode=defaultField")
        argument.extend(extraargs)
        dsetList = []
    
        try:
            result = self.amiclient.execute(argument)
            resultDict= result.getDict()
            resultByRow = resultDict['Element_Info']
            for row, vals in resultByRow.iteritems():
                dsetList.append(str(vals['logicalDatasetName']))
        except Exception, msg:
            print msg

        return dsetList

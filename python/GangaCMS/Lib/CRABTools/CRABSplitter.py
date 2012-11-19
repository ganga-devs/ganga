from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from GangaCMS.Lib.Utils import SplitterError

import xml.dom.minidom
from xml.dom.minidom import parse,Node

class CRABSplitter(ISplitter):
   
    comments = []

    schemadic={}
    schemadic['maxevents']                = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc='')
    schemadic['inputfiles']               = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='')
    schemadic['skipevents']               = SimpleItem(defvalue=None, typelist=['type(None)','int'], doc='')

    _name = 'CRABSplitter'
    _schema = Schema(Version(1,0), schemadic)
    
    def parseArguments(self,path):

        doc = parse(path)   
        jobs = doc.getElementsByTagName("Job")
        splittingData = []
        
        for job in jobs:         
            data = [job.getAttribute("MaxEvents"),
                    job.getAttribute("InputFiles"),
                    job.getAttribute("SkipEvents")] 
            splittingData.append(data)

        return splittingData


    def split(self,job):     

        workdir = job.inputdata.ui_working_dir

        try:
            splittingData = self.parseArguments('%sshare/arguments.xml'%(workdir))
        except IOError,e:
            raise SplitterError(e)

        subjobs=[]
      
        for index in range(len(splittingData)):
            j = self.createSubjob(job)
            j.master = job
            j.application = job.application
            j.inputdata = job.inputdata
            j.backend = job.backend

            splitter = CRABSplitter()
            splitter.maxevents              = splittingData[index][0]
            splitter.inputfiles             = splittingData[index][1]
            splitter.skipevents             = splittingData[index][2]          
            j.splitter = splitter
            subjobs.append( j )
        
        return subjobs

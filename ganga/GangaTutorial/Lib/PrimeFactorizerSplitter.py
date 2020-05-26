################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PrimeFactorizerSplitter.py,v 1.1 2008-07-17 16:41:37 moscicki Exp $
################################################################################

from GangaCore.GPIDev.Schema import *
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Lib.File import File
from GangaCore.GPIDev.Adapters.ISplitter import ISplitter

class PrimeFactorizerSplitter(ISplitter):
    """Job splitter for prime number factorization"""
    
    _name = "PrimeFactorizerSplitter"
    _schema = Schema(Version(1,0), {
        'numsubjobs': SimpleItem(defvalue=1,sequence=0, typelist=['int'], doc="Number of subjobs")
        } )

    ### Splitting based on numsubjobs
    def split(self,job):
        from GangaCore.GPIDev.Lib.Job import Job
        subjobs = []
        primeTables = job.inputdata.get_dataset()

        ## avoid creating jobs with nothing to do
        if self.numsubjobs > len(primeTables):
            self.numsubjobs = len(primeTables)

        ## create subjobs
        for i in range(self.numsubjobs):
            j = Job()
            j.application   = job.application
            j.inputdata     = job.inputdata 
            j.inputdata.table_id_lower = 1 
            j.inputdata.table_id_upper = 1
            j.outputdata    = job.outputdata 
            j.inputsandbox  = job.inputsandbox
            j.outputsandbox = job.outputsandbox
            j.backend       = job.backend
            subjobs.append(j)

        ## chunksize of each subjob
        chunksize = len(primeTables) / self.numsubjobs

        offset = 0
        for i in range(len(subjobs)):
            my_chunksize = chunksize
            if len(primeTables) % self.numsubjobs >= i+1: my_chunksize+=1

            ## set lower bound id (inclusive)
            subjobs[i].inputdata.table_id_lower = int(offset+1)
            ## fill subjob with prime tables 
            #for j in range(my_chunksize):
            #    subjobs[i].application.addPrimeTable(primeTables[offset+j])
            offset += my_chunksize
            ## set upper  bound id (inclusive)
            subjobs[i].inputdata.table_id_upper = int(offset)

        return subjobs

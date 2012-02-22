from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *

class NasimSplitter(ISplitter):

    """
    Split a nasim job by giving the total number of events to generate and
    the number of subjobs to split over
    """

    _name = "NasimSplitter"
    _schema = Schema(Version(1,0), {
        'num_subjobs':  SimpleItem(defvalue=0,doc='The number of subjobs to use')
        } )

    def split(self,job):

        import math, time
        
        subjobs = []

        if self.num_subjobs == 0:
            raise ApplicationConfigurationError(None, "No subjobs selected")

        if self.num_subjobs > 1000:
            raise ApplicationConfigurationError(None, "Greater than 1000 subjobs selected - possible seed problems")

        evts_per_job = job.application.num_triggers

        ijob = 0
        while ijob < self.num_subjobs:
            j = self.createSubjob(job)
            
            # Alter arguments for each subjob
            j.application.job_file = job.application.job_file
            j.application.titles_file = job.application.titles_file
            j.application.beam = job.application.beam
            j.application.num_triggers = job.application.num_triggers
            j.application.run_number = job.application.run_number
            j.application.prod_num = job.application.prod_num
            j.application.seed = j.application.prod_num*100000000+(j.application.run_number-15000)*2000+(j.application.beam-1)*1000+ijob

            j.outputdata.data = job.outputdata.data
            j.outputdata.name = job.outputdata.name
            
            subjobs.append(j)
            ijob += 1
            
        return subjobs


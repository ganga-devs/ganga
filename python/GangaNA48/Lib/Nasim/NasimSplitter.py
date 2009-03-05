from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *

class NasimSplitter(ISplitter):

    """
    Split a nasim job by giving the total number of events to generate and
    the number of subjobs to split over
    """

    _name = "NasimSplitter"
    _schema = Schema(Version(1,0), {
        'total_events': SimpleItem(defvalue=0,doc='The total number of events to generate'),
        'num_subjobs':  SimpleItem(defvalue=0,doc='The number of subjobs to use')
        } )

    def split(self,job):

        import math, time
        
        subjobs = []

        if self.total_events == 0:
            raise ApplicationConfigurationError(None, "No events to generate")

        if self.num_subjobs == 0:
            raise ApplicationConfigurationError(None, "No subjobs selected")

        if self.num_subjobs > 1000:
            raise ApplicationConfigurationError(None, "Greater than 1000 subjobs selected - possible seed problems")

        evts_per_job = math.ceil( self.total_events / self.num_subjobs )
        dataset_name = 'users_run' + str(job.application.run_number) + '_beam' + str(job.application.beam) + '_trigs' + str(self.total_events) + '_' + time.strftime('%b_%a_%m_%d_%H_%M_%S')

        ijob = 0
        while ijob < self.num_subjobs:
            j = self.createSubjob(job)
            
            # Alter arguments for each subjob
            j.application.num_triggers = int(evts_per_job)
            j.application.seed = j.application.prod_num*100000000+(j.application.run_number-15000)*2000+(j.application.beam-1)*1000+ijob
            j.application.dataset_name = dataset_name
            
            subjobs.append(j)
            ijob += 1
            
        return subjobs


import os
import datetime
import time

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['datetime'] = datetime
        
class JobTime(GangaObject):
    """Job timestamp access.
       In development

       Changes in the status of a Job are timestamped - a datetime object
       is stored in the dictionary named 'timestamps', in Coordinated
       Universal Time(UTC). More information on datetime objects can be
       found at:

       http://docs.python.org/library/datetime.html

       Datetime objects can be subtracted to produce a 'timedelta' object.
       More information about these can be found at the above address.
       '+', '*', and '/' are not supported by datetime objects.

       Datetime objects can be formatted into strings using the
       .strftime(format_string) application, and the strftime codes.
       e.g. %Y -> year as integer
            %a -> abbreviated weekday name
            %M -> minutes as inetger

       The full list can be found at:
       http://docs.python.org/library/datetime.html#strftime-behavior

       Standard status types with built in access methods are:
       -'new'
       -'submitted'
       -'running'
       -'completed'
       -'killed'
       -'failed'

       These return a string with default format %Y/%m/%d @ %H:%M:%S. A
       custom format can be specified in the arguement.

       Any information stored within the timestamps dictionary can also be
       extracted in the way as in would be for a standard, non-application
       specific python dictionary.

       For a table display of the Job's timestamps use .time.display(). For
       timestamps details from the backend use .time.details()
       
       
    """

    timestamps = {}
    sj_statlist = []
    
    _schema = Schema(Version(0,0),{'timestamps' : SimpleItem(defvalue={},doc="Dictionary containing timestamps for job", summary_print='_timestamps_summary_print')
                                    })

    _category = 'jobtime'
    _name = 'JobTime'
    _exportmethods = ['display',
                      'new',
                      'submitting',
                      'submitted',
                      'backend_running',
                      'backend_final',
                      'completing',
                      'final',
                      'runtime',
                      'waittime',
                      'submissiontime',
                      'details',
                      'printdetails']
    
    def __init__(self):
        super(JobTime, self).__init__()
        self.timestamps = {}
        self.sj_statlist = [] #this makes sure the contents of the list don't get copied when the Job does.

    def __deepcopy__(self,memo):
        obj = super(JobTime, self).__deepcopy__(memo)
        obj.newjob()
        return obj    

    def newjob(self):
        """Timestamps job upon creation.
        """
        t = datetime.datetime.utcnow()
        self.timestamps['new'] = t 

    def timenow(self, status):
        """Updates timestamps as job status changes.
        """
        j = self.getJobObject()
        t_now = datetime.datetime.utcnow()
        b_list = ['running', 'completing', 'completed', 'failed']
        final = ['killed', 'failed', 'completed']
        backend_final = ['failed', 'completed']
        ganga_master = ['new', 'submitting', 'killed']

        logger.debug("Job %d called timenow('%s')", j.id, status)
                
        #standard method:
        if not j.subjobs: 
            #backend stamps
            if status in b_list: 
                for childstatus in b_list:
                    be_statetime = j.backend.getStateTime(childstatus)
                    if be_statetime != None:
                        if childstatus in backend_final:
                            self.timestamps["backend_final"] = be_statetime 
                            logger.debug("Wrote 'backend_final' to timestamps.")
                        else:
                            self.timestamps["backend_"+childstatus] = be_statetime 
                            logger.debug("Wrote 'backend_%s' to timestamps.", childstatus)
                    if childstatus==status: break
            #ganga stamps
            if status in final:
                self.timestamps["final"] = t_now 
                logger.debug("Wrote 'final' to timestamps.")
            else:
                self.timestamps[status] = t_now 
                logger.debug("Wrote '%s' to timestamps.", status)

        #subjobs method:
        if j.master: #identifies subjobs               
            logger.debug("j.time.timenow() caught subjob %d.%d in the '%s' status", j.master.id, j.id, status)

            for written_status in j.time.timestamps.keys():
                if written_status not in j.master.time.sj_statlist:                 
                    j.master.time.sj_statlist.append(written_status)
                    logger.debug("written_status: '%s' written to sj_statlist", written_status)

        #master job method
        if j.subjobs: #identifies master job
            logger.debug("j.time.timenow() caught master job %d in the '%s' status", j.id, status) 
            if status in ganga_master: ## don't use subjob stamp for these
                self.timestamps[status] = t_now
                logger.debug("status: '%s' in ganga_master written to master timestamps.", status)
            else:
                for state in self.sj_statlist:
                    if state not in ganga_master:
                        j.time.timestamps[state] = self.sjStatList_return(state)
                        logger.debug("state: '%s' from sj_statlist to written to master timestamps.", state)
                    else:
                        pass

    def sjStatList_return(self, status):
        list = []
        final = ['backend_final', 'final']
        j = self.getJobObject()
        for sjs in j.subjobs:
            try:
                list.append(sjs.time.timestamps[status])
            except KeyError:
                logger.debug("Status '%s' not found in timestamps of job %d.%d.", status, sjs.master.id, sjs.id)
        list.sort()
        try:
            if status in final:
                return list[-1]
            return list[0]
        except IndexError:
            logger.debug("IndexError: ID: %d, Status: '%s', length of list: %d", j.id, status, len(list)) #change this to a more appropriate debug.
            pass

    def display(self, format="%Y/%m/%d %H:%M:%S"): ## Justin 10.9.09: I think 'ljust' might be just as good if not better than 'rjust' here:
        """Displays existing timestamps in a table.

           Format can be specified by typing a string of the appropriate strftime() behaviour codes as the arguement.
           e.g. '%H:%M:%S' ==> 13:55:01

           For a full list of codes see
           http://docs.python.org/library/datetime.html?#strftime-behavior
        """
        retstr=''
        T = datetime.datetime.now()
        tstring = T.strftime(format)
        length = len(tstring)
        times = [0 for k in self.timestamps.keys()]
        for i in range(0, len(self.timestamps.keys())):
            try:
                times[i] = self.timestamps[self.timestamps.keys()[i]].strftime(format).rjust(length) + ' - ' + self.timestamps.keys()[i]
            except AttributeError:
                times[i] = str(self.timestamps[self.timestamps.keys()[i]]).rjust(length) + ' - ' + self.timestamps.keys()[i]

        times.sort() ##try to make chronological - can fail when timestamps are the same to nearest sec -> becomes alphabetical...
        retstr = retstr + '\n' + 'Time (UTC)'.rjust(length) + '   Status' + '\n'
        for i in range(0,21):
            retstr = retstr + '- '
        retstr = retstr+'\n'
        for i in range (0, len(times)):
            retstr = retstr + times[i] + '\n' 
        return retstr

    def _timestamps_summary_print(self,value,verbosity_level):
        """Used to display timestamps when JobTime object is displayed.
        """
        return self.display()

    #This didn't work:
    #
    #def __str__(self):
    #    """ string cast """
    #    return self.display()
    
    def details(self, subjob=None):
        """Obtains all timestamps available from the job's specific backend.

           Subjob arguement: None  = default
                             'all' = gets details for ALL SUBJOBS. You have been warned.
                             int   = gets details for subjob number 'int'
    
           No argument is required for a job with no subjobs.    
        """
        j = self.getJobObject()
        idstr = ''
        detdict = {}

        #If job is SUBJOB do the normal procedure. Not sure this clause is neccessary as subjobs will be caught normally
        if j.master:                                 
            logger.debug("j.time.details(): subjob %d.%d caught.", j.master.id, j.id)
            detdict = j.backend.timedetails()
            return detdict                              

        #If job is MASTER iterate over subjobs and do normal method. This isn't going to be ideal for a large number of subjobs
        if j.subjobs:
            logger.debug("j.time.details(): master job %d caught.", j.id)
            idstr = str(j.id)

            # User wants 'all'
            if subjob == 'all':
                keyin = None

                # NOTE: The interactive loop below was more an exercise for learning how 'keyin' is used than a useful addition.
                # ask whether user really wants to print timedetails for all their jobs:
                while keyin == None: 
                    keyin = raw_input("Are you sure you want details for ALL %d subjobs(y/n)?" %len(j.subjobs))
                    #if yes carry on at for loop
                    if keyin == 'y': 
                        pass
                    #if no return None. Doesn't execute rest of method
                    elif keyin == 'n': 
                        return None
                    #if something else - asks again
                    else:              
                        print "y/n please!"
                        keyin = None

                for jobs in j.subjobs: 
                    subidstr = idstr + '.' + str(jobs.id)
                    logger.debug("Subjob: %d, Backend ID: %d", jobs.id, jobs.backend.id) #String needs more info if it is going to stay in.
                    detdict[subidstr] = jobs.backend.timedetails()
                return detdict

            # no arguement specified
            elif subjob == None:
                logger.debug("j.time.details(): no subjobs specified for this master job.") 
                return None

            # Subjob id or string passed
            else:
               # string = error
               if type(subjob) != int:
                   raise TypeError("Subjob id requires type 'int'")
               # subjob id supplied
               for sj in j.subjobs:
                   if sj.id == subjob:
                       logger.debug("Subjob: %d, Backend ID: %d", sj.id, sj.backend.id)
                       detdict = sj.backend.timedetails()
                       return detdict
                   else:
                       pass
               if subjob >= len(j.subjobs):
                   logger.warning("Index '%s' is out of range. Corresponding subjob does not exist.", str(subjob))
                   return None
               
            logger.debug("subjob arguement '%s' has failed to be caught and dealt with.", subjob)
            return None  

        detdict = j.backend.timedetails() ## called if no subjobs
        return detdict

    def printdetails(self, subjob=None):
        """Prints backend details to screen by calling details() and printing the returned dictionary.
        """
        j = self.getJobObject()
        if subjob == 'all':
            #the warning and action taken below are pretty annoying, but I was unsure how to deal with the request to print the details for all n subjobs, which seems unlikely to be made.
            logger.warning("It might be unwise to print all subjobs details. Use details() and extract relevant info from dictionary.")
            return None
        pd = self.details(subjob)
        for key in pd.keys():
            print key, '\t', pd[key]

    def runtime(self):
        """Method which returns the 'runtime' of the specified job.

           The runtime is calculated as the duration between the job entering the 'running' state and the job entering the 'completed' state.
        """
        end_list = ['killed', 'completed', 'failed']
        end_stamps = {}

        # if master job, sum: 
        j = self.getJobObject()
        if j.subjobs:
            masterrun = datetime.timedelta(0, 0, 0)
            for jobs in j.subjobs:
                masterrun = masterrun + jobs.time.runtime()
            return masterrun
        #all other jobs:
        return self.duration('backend_running', 'backend_final')
        
    def waittime(self):
        """Method which returns the waiting time of the specified job.

           The waiting time is calculated as the duration between the job entering the 'submitted' state and entering the 'running' state.
        """
        #master job:
        j = self.getJobObject()        
        if j.subjobs: 
            start_list = []
            end_list = []
            for jobs in j.subjobs:
                start_list.append(jobs.time.timestamps['submitted'])
                end_list.append(jobs.time.timestamps['backend_running'])
            start_list.sort()
            end_list.sort()
            start = start_list[0]
            end = end_list[len(end_list) - 1]
            masterwait = end - start
            return masterwait
        #all other jobs:
        return self.duration('submitted', 'backend_running')

    def submissiontime(self):
        """Method which returns submission time of specified job.

           Calculation: sub_time =  submitted - submitting.
        """
        j = self.getJobObject()
        if j.subjobs:
            start_list = []
            end_list = []
            for jobs in j.subjobs:
                end_list.append(jobs.time.timestamps['submitted'])
            end_list.sort()
            start = j.time.timestamps['submitting']
            end = end_list[len(end_list) - 1]
            mastersub = end - start
            return mastersub
        return self.duration('submitting', 'submitted')

    def duration(self, start, end):
        """Returns duration between two specified timestamps as timedelta object.
        """
        if start in self.timestamps.keys():
            if end in self.timestamps.keys():
                s,e = self.timestamps[start],self.timestamps[end]
                s_micro, e_micro = datetime.timedelta(0, 0, s.microsecond), datetime.timedelta(0, 0, e.microsecond)
                e,s = e-e_micro, s-s_micro
                td =  e-s

               #method for rounding removed because timestamps aren't always recorded with microsecond precision, and stamping accuracy isn't high enough to justify doing so 
#                ds = td.days
 #               secs = td.seconds
#                micros = td.microseconds
 #               if micros >= 500000:
  #                  secs +=1

                dur = td #datetime.timedelta(days=ds, seconds=secs)
                return dur
            else:
                logger.warning("Could not calculate duration: '%s' not found.", end)
        else:
            logger.warning("Could not calculate duration: '%s' not found.", start)
        return None
        


    def statetime(self, status, format=None):
        """General method for obtaining the specified timestamp in specified format.
        """
        if status not in self.timestamps:
            logger.debug("Timestamp '%s' not available.", status)
            return None
        if format != None:
            return self.timestamps[status].strftime(format)
        return self.timestamps[status]

    def new(self, format=None):
        """Method for obtaining 'new' timestamp.
        """
        return self.statetime('new', format)

    def submitting(self, format=None):
        """Method for obtaining 'submitting' timestamp.
        """
        return self.statetime('submitting', format)

    def submitted(self, format=None):
        """Method for obtaining 'submitted' timestamp.
        """
        return self.statetime('submitted', format)

    def backend_running(self, format=None):
        """Method for obtaining 'backend_running' timestamp.
        """
        return self.statetime('backend_running', format)

    def backend_final(self, format=None):
        """Method for obtaining 'backend_final' timestamp.
        """        
        return self.statetime('backend_final', format)

    def completing(self, format=None):
        """Method for obtaining 'completing' timestamp.
        """        
        return self.statetime('completing', format)

    def final(self, format=None):
        """Method for obtaining 'final' timestamp.
        """        
        return self.statetime('final', format)  
    

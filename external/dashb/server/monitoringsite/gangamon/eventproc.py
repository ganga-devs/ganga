from monitoringsite.gangamon.models import *
import time

class GangaEventProcessor:
    def __init__(self):
        pass

    def process_event(self,data):
        _msg_t = data[0]
        event = data[1]
        msg = data[2]
        
        try:
            g = GangaJobDetail.objects.get(job_uuid=msg['ganga_job_uuid'])
        except: #IntegrityError
            g = GangaJobDetail()
        g.job_id = msg['ganga_job_id'] 
        g.master_uuid = msg['ganga_master_uuid']
        g.num_subjobs = msg['subjobs'] 
        g.job_uuid = msg['ganga_job_uuid']
        g.job_name = msg['job_name']
        g.application = msg['application']
        g.backend = msg['backend']
        g.repository = msg['ganga_user_repository']
        # extract user from repository as chars before first '@'
        g.user = ''
        if '@' in g.repository:
            g.user = g.repository[:g.repository.find('@')]
        
        if g.host != '':
            if g.master_uuid == '0' and g.host != msg['hostname']:
                g.ce = msg['hostname']
            elif g.host != msg['hostname']:
                g.workernode = msg['hostname']
        else:
            g.host = msg['hostname']
        
        #g.host = msg['hostname']
        g.last_status = event
        g.last_timestamp = int(_msg_t)
        g.save()

        s = GangaJobStatus()
        s.event = g
        s.timestamp = int(_msg_t)
        s.status = event
        s.save()

class DianeEventProcessor:
    def __init__(self):
        self.events = {}
        self.tasks = {}
    
    def createOrGetRunObject(self, master_uuid):
        '''Creates a DianeRun object and fills the data fields with default 'unknown' values. These are later
           repaced by data from the processed events. In case the processed events do not contain the required
           information, default value is stored in the database'''
        try: # get the run object from the database
            run = DianeRun.objects.get(master_uuid=master_uuid)
        except: # or create a new one if it doesn't exist
            run = DianeRun()
            run.master_uuid = master_uuid
            run.name = 'unknown'
            run.application = 'unknown'
            run.rid = -1
            run.runid = 'unknown'
            run.wn_total = 0
            run.wn_now = 0
            run.tasks_total = 0
            run.tasks_completed = 0
            run.ganga_job_uuid = 'unknown'
            run.host = 'unknown'
            run.start_time = 0
            run.user = 0
        
        run.save()
        return run

    def createOrGetTaskObject(self, run, tid):
        '''Creates a DianeTask object and fills the data fields with default 'unknown' values unless otherwise specified.'''
        try: # get the task object from the database
            task = DianeTask.objects.get(run=run,tid=tid)
        except: # or create a new one if it doesn't exist
            task = DianeTask()
            task.tid = tid
            task.wid = 0
            task.run = run
            task.status = 'new'
            task.execution_count = 0
            task.application_label = ''
        task.save()
        return task

    def createStringLabel(self, task, label, value):
        '''Creates a DianeStringLabel object.'''
        strlabel = DianeStringLabel()
        strlabel.task = task
        strlabel.label = label
        strlabel.value = value

        strlabel.save()
        return strlabel

    def createIntLabel(self, task, label, value):
        '''Creates a DianeIntLabel object.'''
        intlabel = DianeIntLabel()
        intlabel.task = task
        intlabel.label = label
        intlabel.value = value
        intlabel.save()
        return intlabel

    def createFloatLabel(self, task, label, value):
        '''Creates a DianeFloatLabel object.'''
        floatlabel = DianeFloatLabel()
        floatlabel.task = task
        floatlabel.label = label
        floatlabel.value = value

        floatlabel.save()
        return floatlabel

    def process_event(self, event):
        '''Updates the run information with data extracted from a single event. If autosave=True,
           data will be saved in the database automatically. Additionally, event itself is also
           saved as a DianeEvent object.'''

        timestamp = event[0]
        event_type = event[1]
        msg = event[2]

        try:
            master_uuid = msg['_master_uuid']
        except KeyError: # some events don't have the master uuid
            return

        run = self.createOrGetRunObject(master_uuid)

        # modifications to the run object will be automatically saved in the finally clause
        try:

            if event_type == 'master_start':
                run.application = msg['application_name']
                run.name = msg['name']
                run.runid = msg['runid']
                run.user = msg['runid'].split('@')[0]
                run.host = msg['runid'].split('@')[1].split(':')[0]
                run.rid = int(msg['runid'].split('/')[-2])
                run.start_time = timestamp 
                return

            if event_type == 'worker_registered':
                # add some redundancy to master_uuid just in case first message goes missing
                run.master_uuid = master_uuid
                run.wn_total += 1
                run.wn_now += 1
                return

            if event_type == 'worker_removed':
                run.wn_now -= 1
                return

            # here we handle the fact that some messages may send information about one or more tasks
            # the distiction is made by looking into 'tids' and 'tid' message attributes
            try:
                tids = msg['tids']
            except KeyError:
                try:
                    tids = [msg['tid']]
                except KeyError:
                    tids = []

            #print '*'*30
            #print 'TASK EVENT'
            #print 'event_type',event_type
            #print 'tids',tids
            #print 'msg',msg
            #print '*'*30

            # let's get (or create) task objects
            tasks = []
            for tid in tids:

                task = self.createOrGetTaskObject(run,tid)

                #print 'processing task',task.tid,task.wid,task.status,task.execution_count,task.application_label

                # these values might or might not be present, depending on the event type
                try:
                    task.wid = msg['wid']
                except KeyError:
                    pass

                if event_type == 'put_task_result':
                    if not msg['error']:
                        #print 'OK TASK'
                        run.tasks_completed += 1
                        task.status = 'completed'
                    else:
                        #print 'FAILED TASK'
                        task.status = 'incomplete'

                if event_type == 'get_task_data':
                    task.status = 'running'
                    task.execution_count += 1

                if event_type == 'tasks_ignored':
                    task.status = 'failed'

                if event_type == 'tasks_lost':
                    task.status = 'incomplete'

                if event_type == 'new_task_created':
                    run.tasks_total += 1
                    task.status = 'new'
                    try:
                        task.application_label = msg['application_label']
                    except KeyError:
                        pass #older versions of DIANE do not publish application_labels...

                    details = []
                    try:
                        details = msg['application_details']
                    except Exception,x:
                        print "problem parsing the task details:",x,repr(msg['application_details'])

                    try:
                        for key,value in details:
                            if type(value) is type(1.0):
                                label = self.createFloatLabel(task,key,value)
                            elif type(value) is type(1):
                                label = self.createIntLabel(task,key,value)
                            else:
                                label = self.createStringLabel(task,key,str(value))
                    except TypeError,x:
                        print "problem iterating key,value pairs of the task details",x,repr(msg['application_details'])
                    
                    #print 'saving task',task.tid,task.wid,task.status,task.execution_count,task.application_label
                task.save()

            if event_type.startswith('_'): # worker events
                try:
                    ganga_job_uuid = msg['ganga_job_uuid'] # if we have information about ganga job
                    try:
                        j = GangaJobDetail.objects.get(job_uuid = ganga_job_uuid)
                        j.diane_master_uuid = master_uuid
                        j.save()
                    except:
                        pass
                except KeyError:
                    pass
        finally:
            # save everything we did
            run.save()



            
            
            

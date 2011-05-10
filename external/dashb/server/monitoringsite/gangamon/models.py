from django.db import models

class GangaJobDetail(models.Model):
    get_latest_by = 'last_timestamp'
    job_id = models.CharField(max_length=100)
    job_uuid = models.CharField(max_length=100, primary_key=True)
    job_name = models.CharField(max_length=100)
    num_subjobs = models.IntegerField()
    master_uuid = models.CharField(max_length=100)
    application = models.CharField(max_length=50)
    backend = models.CharField(max_length=100)
    user = models.CharField(max_length=50)
    repository = models.CharField(max_length=200)
    host = models.CharField(max_length=200)
    ce = models.CharField(max_length=200)
    workernode = models.CharField(max_length=200)
    last_status = models.CharField(max_length=100)
    last_timestamp = models.IntegerField()
    diane_master_uuid = models.CharField(max_length=200, null=True) # may be null

class GangaJobStatus(models.Model):
    get_latest_by = 'timestamp'
    event = models.ForeignKey(GangaJobDetail)
    timestamp = models.IntegerField()
    status = models.CharField(max_length=200)
    
    def __unicode__(self):
        return u'%s %s %s' % (self.event.job_uuid, self.timestamp, self.status)

# The main run
class DianeRun(models.Model):
    master_uuid = models.CharField(max_length=200, primary_key=True) # Master UUID, primary key
    name = models.CharField(max_length=200) # Name of the running program or runfile
    application = models.CharField(max_length=200) # Name of the running program
    user = models.CharField(max_length=50) # The user
    rid = models.IntegerField() # The local run ID
    runid = models.CharField(max_length=200) # The global (unique) run ID
    wn_total = models.IntegerField() # Total number of workers
    wn_now = models.IntegerField() # Number of workers working now
    tasks_total = models.IntegerField() # Total number of tasks
    tasks_completed = models.IntegerField() # Number of complete tasks
    host = models.CharField(max_length=200) # The host 
    start_time = models.IntegerField() # The time when the run started

# The tasks of which the run consists of
class DianeTask(models.Model):
    tid = models.IntegerField() # The task ID
    wid = models.IntegerField(null=True) # The worker ID of the worker which processes the task
    run = models.ForeignKey(DianeRun) # The run of which the task is part
    status = models.CharField(max_length=20) # The current status of the task (new, running, incomplete, completed, failed)
    execution_count = models.IntegerField() # The number of times the task has been attempted. 
    application_label = models.CharField(max_length=200,null=True) # A task-specific label identifying it from all the others 

# # Events that happens during the run of the tasks, such as messages given
# class DianeEvent(models.Model):
#     event = models.CharField(max_length=200) # The label of the event, describing the event type
#     timestamp = models.IntegerField() # The time the event occured
#     master_uuid = models.CharField(max_length=200) # The UUID of the master
#     worker_uuid = models.CharField(max_length=200,null=True) # The UUID of the worker
#     wid = models.IntegerField(null=True) # The ID of the worker for which the event occured (if applicable)
#     run = models.ForeignKey(DianeRun) # The run in which the event occured
#     task = models.ForeignKey(DianeTask,null=True) # The task in which the event occured (if applicable)
#     tid = models.IntegerField(null=True) # The ID of the task in which the event occured
#     ganga_job_uuid = models.CharField(max_length=200,null=True) # The UUID of the Ganga job in which the event occured (if applicable)
#     host = models.CharField(max_length=200,null=True) # The host on which the event occured


# Classes to be used to extend the application label
# Use these classes to add multiple identifying parameters to the tasks

class DianeIntLabel(models.Model):
    task = models.ForeignKey(DianeTask)
    label = models.CharField(max_length=200)
    value = models.IntegerField()

class DianeFloatLabel(models.Model):
    task = models.ForeignKey(DianeTask)
    label = models.CharField(max_length=200)
    value = models.FloatField()

class DianeStringLabel(models.Model):
    task = models.ForeignKey(DianeTask)
    label = models.CharField(max_length=200)
    value = models.CharField(max_length=200)


class Users(models.Model):
    name = models.CharField(max_length=50)
    dianeuser = models.BooleanField()
    gangauser = models.BooleanField()

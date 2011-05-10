from django.db import models

class GangaJobDetail(models.Model):
    get_latest_by = 'latext_timestamp'
    job_id = models.IntegerField()
    job_uuid = models.CharField(max_length=100)
    job_name = models.CharField(max_length=100)
    master_uuid = models.CharField(max_length=100)
    application = models.CharField(max_length=50)
    backend = models.CharField(max_length=100)
    user = models.CharField(max_length=50)
    repository = models.CharField(max_length=200)
    host = models.CharField(max_length=200)
    last_status = models.CharField(max_length=100)
    last_timestamp = models.IntegerField()

class GangaJobStatus(models.Model):
    get_latest_by = 'timestamp'
    event = models.ForeignKey(GangaJobDetail)
    timestamp = models.IntegerField()
    status = models.CharField(max_length=200)

class DianeEvent(models.Model):
    event = models.CharField(max_length=200)
    timestamp = models.DateTimeField(auto_now=True)
    master_uuid = models.CharField(max_length=200)
    worker_uuid = models.CharField(max_length=200)
    worker_id = models.IntegerField()
    run_id = models.CharField(max_length=200)
    task_id = models.IntegerField()
    ganga_job_uuid = models.CharField(max_length=200)
    host = models.CharField(max_length=200)

class DianeRun(models.Model):
    master_uuid = models.CharField(max_length=200)
    name = models.CharField(max_length=200)
    application = models.CharField(max_length=200)
    rid = models.IntegerField()
    runid = models.CharField(max_length=200)
    wn_total = models.IntegerField()
    wn_now = models.IntegerField()
    tasks_total = models.IntegerField()
    tasks_completed = models.IntegerField()
    ganga_job_uuid = models.CharField(max_length=200)
    host = models.CharField(max_length=200)

class DianeEventProcessor():
    def __init__(self):
        self.run = DianeRun()
        self.run.application = 'dummy'
        self.run.name = 'dummy_name'
        self.run.wn_total = 0
        self.run.wn_now = 0
        self.run.tasks_total = 0
        self.run.tasks_completed = 0
        self.ganga_job_uuid = 'ASDFGHJ'
        self.host = 'localhost'

    def process_event(self, event):
        if event[1] == 'master_start':
            self.run.master_uuid = event[2]['_master_uuid']
            self.run.runid = event[2]['runid']
            self.run.rid = int(event[2]['runid'].split('/')[-2])
        elif event[1] == 'worker_registered':
            self.run.wn_total += 1
            self.run.wn_now += 1
        elif event[1] == 'worker_removed':
            self.run.wn_now -= 1
        elif event[1] == 'worker_ping':
            pass
        elif event[1] == 'get_init_data':
            pass
        else:
            pass
        try:
            self.run.save()
        except: # not enough data yet
            pass

class DianeIntArg(models.Model):
    event = models.ForeignKey(DianeEvent)
    label = models.CharField(max_length=200)
    value = models.IntegerField()

class DianeFloatArg(models.Model):
    event = models.ForeignKey(DianeEvent)
    label = models.CharField(max_length=200)
    value = models.FloatField()

class DianeStringArg(models.Model):
    event = models.ForeignKey(DianeEvent)
    label = models.CharField(max_length=200)
    value = models.CharField(max_length=200)

from django.db import models
import base64

# Create your models here.
class Report(models.Model):
        username = models.CharField(max_length=30)
        date_uploaded = models.DateTimeField('date uploaded')
        ganga_version = models.CharField(max_length=50)
        job_uuid = models.CharField(max_length=100)
        is_deleted = models.BooleanField(default=False)

class MonitoringLink(models.Model):
        report = models.ForeignKey(Report)
        monitoring_link_path = models.CharField(max_length=500)
        monitoring_link_name = models.CharField(max_length=100)


        

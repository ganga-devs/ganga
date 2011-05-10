from django.db import models

# Create your models here.


from django.db import models

class GangaSession(models.Model):
     get_latest_by = 'time_start'
     version = models.CharField(max_length=100)
     time_start = models.IntegerField(db_index=True)
     host = models.CharField(max_length=255)
     session_type = models.CharField(max_length=50)
     user = models.CharField(max_length=255)
     runtime_packages = models.CharField(max_length=255)
     # extended attributes
     interactive = models.NullBooleanField()
     GUI = models.NullBooleanField()
     webgui = models.NullBooleanField()
     script_file = models.NullBooleanField()
     test_framework = models.NullBooleanField()
     text_shell = models.CharField(max_length=50)

# mysql schema migration
#
# ALTER TABLE gangausage_gangasession ADD COLUMN interactive bool;
# ALTER TABLE gangausage_gangasession ADD COLUMN GUI bool;
# ALTER TABLE gangausage_gangasession ADD COLUMN webgui bool;
# ALTER TABLE gangausage_gangasession ADD COLUMN script_file bool;
# ALTER TABLE gangausage_gangasession ADD COLUMN test_framework bool;
# ALTER TABLE gangausage_gangasession ADD COLUMN text_shell varchar(50) NOT NULL;


class GangaJobSubmitted(models.Model):
     
     application = models.CharField(max_length=100)     
     backend = models.CharField(max_length=100) 
     host = models.CharField(max_length=255)
     user = models.CharField(max_length=255)
     date = models.DateField()  
     plain_jobs = models.IntegerField()
     master_jobs = models.IntegerField()
     sub_jobs = models.IntegerField()
     runtime_packages = models.CharField(max_length=255)


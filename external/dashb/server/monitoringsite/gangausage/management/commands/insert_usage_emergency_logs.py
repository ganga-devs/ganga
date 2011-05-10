from django.core.management.base import BaseCommand

from monitoringsite.gangausage.models import *

class Command(BaseCommand):
    help = "Insert usage records from emergency log"

    requires_model_validation = True 

    def __init__(self):
        pass

    def handle(self, *args, **options):
        #print args,options

        f = file('/data/django/data/ganga.usage.emergency.log')

        print 'reading file',f.name

        cnt = 0

        for line in f:
            params = eval(line)
            s = GangaSession()
            s.time_start = int(params['start']/1000)
            s.session_type = params['session']
            s.version = params['version']
            s.host  = params['host']
            s.user = params['user']
            s.runtime_packages = params['runtime_packages']
            s.save()
            cnt += 1

        print 'Number of records:',cnt

        print 'You should now rename and archive the file /data/django/data/ganga.usage.emergency.log!'

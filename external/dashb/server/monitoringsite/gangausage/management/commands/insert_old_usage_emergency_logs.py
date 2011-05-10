from django.core.management.base import BaseCommand

from monitoringsite.gangausage.models import *

class Command(BaseCommand):
    help = "Insert usage records from old emergency log"

    requires_model_validation = True 

    def __init__(self):
        pass

    def handle(self, *args, **options):
        #print args,options

        f = file('/data/django/data/old_monalisa_logs_2007_2009.dat')

        print 'reading file',f.name

        cnt = 0

        last_ts = 0

        for line in f:
                params = line.split(',')
                s = GangaSession()

                if params[0].find('e+') > 0:
                        floatDate = float(params[0])/1000
                        s.time_start = int(floatDate)
                else:
                        floatDate = float(params[0])
                        s.time_start = int(floatDate)
                s.session_type = params[4]
                s.version = params[3]
                s.host  = params[2]
                s.user = params[1]
                s.runtime_packages = params[5]
                if s.time_start < last_ts:
                    print 'ERROR: timestamps NOT IN ORDER',last_ts
                s.save()
                cnt += 1
                if cnt%10000 == 0:
                    print cnt

        print 'Number of records:',cnt



from django.core.management.base import BaseCommand
from monitoringsite.gangamon.models import *

class Command(BaseCommand):

    help = "Updates the user list"
    def handle(self, *args, **options):
        allusers = Users.objects.all()
        dianeusers = allusers.filter(dianeuser=True)
        gangausers = allusers.filter(gangauser=True)

        #print dianeusers.__len__()
        #print gangausers.__len__()

        cnt = 0
        print "Scanning Diane runs..."
        for ele in DianeRun.objects.values_list('user', flat=True).distinct():
            if ele and not allusers.filter(name=ele):
                print 'Brand new Diane user detected: ',ele
                u = Users(name=ele, dianeuser=True)
                u.save()
                cnt += 1
            elif ele and not dianeusers.filter(name=ele):
                print 'Ganga user using Diane too: ',ele
                allusers.filter(name=ele).update(dianeuser='True')
                cnt += 1
        print '%d new Diane user(s) detected'%cnt

        cnt = 0
        print "Scanning Ganga runs..."
        for ele in GangaJobDetail.objects.values_list('user', flat=True).distinct():
            if ele and not allusers.filter(name=ele):
                print 'Brand new Ganga user detected: ',ele
                u = Users(name=ele, gangauser=True)
                u.save()
                cnt += 1
            elif ele and not gangausers.filter(name=ele):
                print 'Diane user using Ganga too: ',ele
                allusers.filter(name=ele).update(gangauser='True')
                cnt += 1
        print '%d new Ganga user(s) detected'%cnt

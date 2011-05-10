from monitoringsite.gangamon.models import *
import datetime


from django.db.models import Avg, Max, Min, Count

# EGI reporting quarters

may1=1272672000
aug1=1280620800
nov1=1288569600


def diane_report(t1,t2):
    runs = DianeRun.objects.filter(start_time__gte=t1,start_time__lte=t2)
    print 'reporting period',datetime.datetime.fromtimestamp(t1),datetime.datetime.fromtimestamp(t2)
    print 'number of runs', len(runs)
    
    def unique_users(runs):
        users = {}
        for r in runs:
            users.setdefault(r.user,0)
            users[r.user] += 1
        return users
        
    users = unique_users(runs)          
    print 'number of unique users',len(users)
    for u in users:
        print u,users[u]

    tasks_total = 0
    tasks_completed = 0
    wn_total = 0
    wn_now = 0
    host_domains = {}
    for r in runs:
        tasks_total += r.tasks_total
        tasks_completed += r.tasks_completed
        wn_total += r.wn_total
        wn_now += r.wn_now
        h = r.runid.split(':')[0].split('@')[1]
        d = '.'.join(h.split('.')[1:])
        host_domains.setdefault(d,0)
        host_domains[d] += 1

    print ''
    
    print 'number of tasks processed', tasks_total, tasks_completed    
    print 'number of worker nodes used', wn_total, wn_now        
    print 'host domains',len(host_domains)
    for d in host_domains:
         print d,host_domains[d]

    return runs

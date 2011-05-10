import time
from gangausage.models import *
N_days = 365
sessions = GangaSession.objects.filter(time_start__gte=(time.time()-N_days*24*3600))
GUI_sessions= filter(lambda x:x.session_type=='GUI',sessions)

GUI_users = {}

for s in GUI_sessions:
    GUI_users.setdefault(s.user,[])
    GUI_users[s.user].append((s.host,s.runtime_packages))

def unique_hosts(a):
    return list(set(['.'.join(x[0].split('.')[1:]) for x in a]))

def exps(a):
    exps = set()
    for x in a:
        if 'Atlas' in x[1]:
            exps.add('Atlas')
        if 'LHCb' in x[1]:
            exps.add('LHCb')
    return exps

for u in GUI_users:
    a = GUI_users[u]
    print '%3d %10s %30s %15s'%(len(a),u,' '.join(unique_hosts(a)), ' '.join(exps(a)))
    
print '#number of unique GUI users:', len(set(GUI_users.keys()))    

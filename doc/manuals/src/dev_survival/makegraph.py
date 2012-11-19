
# generate job status transition graph
# usage:
#    1) run ganga and this file as a script
# or 2) cd ganga/python, run python interactively and execfile(this_file)

ARC_LABELS = True
STYLED_EDGES = True
DEBUG = False

from Ganga.GPIDev.Lib.Job import Job
import os

g = Job.status_graph
initial_states = Job.initial_states
transient_states = Job.transient_states

import os.path
dot_file = os.path.abspath('ganga_job_stat.dot')
out_type = 'gif'
out_file = dot_file.replace('.dot','.'+out_type)

def debug(s):
    if DEBUG:
        print 'DEBUG:',s

f = file(dot_file,'w')
print >> f, 'digraph JobStatus {'
for node in g:
    debug('src state: %s'%node)
    for dest in g[node]:
        debug('dest state: %s'%dest)        
        LAB = []
        label = g[node][dest].transition_comment
        if ARC_LABELS:
            LAB.append('label="%s"'%label)
            LAB.append('fontsize=8')
        if STYLED_EDGES:
            if label.find('force') != -1:
                LAB.append('style=dotted bold')
        if LAB:
            LAB = '['+','.join(LAB)+']'
        print >>f, '%s -> %s %s;' % (node,dest,LAB)

print >>f,"__start [shape=point]"

for node in initial_states:
    print >>f, '__start -> %s;'%node

for node in transient_states:
    print >>f, '%s [style=filled]'%node
    
print >>f, '}'
f.close()

print 'created', dot_file

#graphviz_top = '/afs/cern.ch/sw/lcg/external/graphviz/1.9/rh73_gcc32/'
#os.system('export LD_LIBRARY_PATH=%s.lib/graphviz:$LD_LIBRARY_PATH; %s/bin/dot -T%s %s -o%s'% (graphviz_top,graphviz_top,out_type, dot_file, out_file))

os.system('dot -T%s %s -o%s'% (out_type, dot_file, out_file))
print 'created', out_file

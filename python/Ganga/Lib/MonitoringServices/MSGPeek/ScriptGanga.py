from time import sleep
import filecmp
import difflib
import sys

NUM_LINES = 300
JOB_A = "/afs/cern.ch/user/m/mchamber/Ganga/install/Ganga-MSG-branch/python/Ganga/Lib/MonitoringServices/MSGPeek/generatesChunk.py"
JOB_B = "/afs/cern.ch/user/m/mchamber/Ganga/install/Ganga-MSG-branch/python/Ganga/Lib/MonitoringServices/MSGPeek/client.py"

num_pairs = int(sys.argv[1])
sys.argv[1]
j = 0
l_p = []
l_c = []

for k in range(num_pairs):
    job_a = Job()  
    job_a.application = Executable(exe=File(JOB_A), args=str(NUM_LINES))
    job_a.backend = LSF()
    job_a.submit()
    l_p.append(job_a)

for k in range(num_pairs) : 
    job_b = Job(name="consumer")   
    job_b.application = Executable(exe=File(JOB_B), args=['-I', str(l_p[k].id)])#,'-S',str(num_pairs)] )
    job_b.backend = LSF()
    job_b.submit()
    l_c.append(job_b)
    
print l_p, l_c
    
for j in range(len(l_c)) :
    job_a = l_p[j]
    job_b = l_c[j]
    print "iteration: ", j
    while (job_a.status,job_b.status) != ("completed","completed"):
        sleep(1)
        print job_a.status, job_b.status
    
    b = job_b.outputdir+'stdout'
    #    b1 = open(b,'r')
    #    print b1.read()
    #job_b.peek("stdout","tail -n 5")
    
    a = job_a.outputdir+'stdout'
    #    a1 = open(a,'r')
    #    print a1.read()
    #job_a.peek("stdout","tail -n 5")
        
    if not filecmp.cmp(a,b) :
        fcollector = open(b,'r')
        fouput = open(a,'r')
        line_co = fcollector.readlines()
        line_out = fouput.readlines()
    
    #    for line in difflib.ndiff(line_co, line_out):
    #        print line,
    
        diff = difflib.context_diff(line_co, line_out)
        sys.stdout.writelines(diff)
    
        if diff is None :
            print "Job collector %s and Job producer %s: Test passed" % (job_a.id, job_b.id)
    
        fcollector.close()
        fouput.close()
    else :
        print "Job collector %s and Job producer %s: Test passed" % (job_a.id, job_b.id)

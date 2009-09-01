import sys
from time import sleep
import sys

""" Run jobs of half and hour, 
Each 500 lines sleeps 5 seconds, 30 minutes are 1800 sec, divided by 5" and multiply by 500, 
in total 180000 lines
for running 50 producers and 50 consumers the NUM_LINES has to be longer tan 13000 in my slow machine
"""
NUM_LINES = 5000
DIRECTORY = "/afs/cern.ch/user/m/mchamber/Ganga/install/Ganga-MSG-branch/python/Ganga/Lib/MonitoringServices/MSGPeek/"
JOB_A = DIRECTORY+'generatesChunk.py' 
JOB_B = DIRECTORY+"client.py"


j = Job(name="Dummy")
j.submit()

start_job = jobs[-1].id +1
num_pairs = int(sys.argv[1])


f = open('Temp.log','w')
ferr = open('ErrorsResultsTests.log','w')


for k in range(num_pairs):
    job_a = Job(name="producer")  
    job_a.application = Executable(exe=File(JOB_A), args=str(NUM_LINES))
    job_a.backend = LCG()
    job_a.submit()

#------------------------ CONSUMERS -----------------------------------------
sys.path.insert(0,"/afs/cern.ch/user/m/mchamber/Ganga/install/Ganga-MSG-branch/python/Ganga/Lib/MonitoringServices/MSGPeek/")
sys.path.insert(0,'.')
processed = {}
k = 0
while  len(processed) < num_pairs:
#   print processed, len(processed)
    j_status = jobs[start_job+k].status
    id = jobs[start_job].id+k
    if j_status == "running" and not processed.has_key(id): 
        """ The client (consumer) will start running after the job (producer) start running"""
        job_b = Job(name="consumer of %d"%(id) )
        job_b.application = Executable(exe=File(JOB_B), args=['-I', str(start_job+k)])     
        job_b.inputsandbox = [File(DIRECTORY+'MSGPeekCollector.py',subdir='_python'), File(DIRECTORY+'stomp.py',subdir='_python')]
        job_b.backend = LCG()
        job_b.submit()
        processed[id]=job_b.id
    elif (j_status == "completed" or j_status == "failed") and not processed.has_key(id):
        ferr.write('The job %d is %s so no client can be launched in that state\n' %(id, j_status))
        processed[id]=-1
    else:
        sleep(1)
    k = k+1
    if k >= num_pairs :
        k=0
f.write(str(processed))
f.close()
ferr.close()

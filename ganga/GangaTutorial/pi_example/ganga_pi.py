#[gtutor20@egee-ui ~]$ cat ganga_pi.py

from __future__ import print_function

print("Hello, this is ganga_pi... loading...")

def read_stdout(j):
    return file(j.outputdir+'/stdout').read()

def compute_pi(j):
    pi = 0
    for s in j.subjobs:
        pi += float(read_stdout(s))
    return pi


NINTERVALS = 10000000
NTASKS = 10

def create_pi_job(N=NTASKS,M=NINTERVALS):
    j = Job()
    j.name='ganga_pi_%d_%d'%(N,M)
    j.application.exe=File('./pi')
    j.splitter=ArgSplitter()
    j.splitter.args=[[str(i),str(N),str(M)] for i in range(N)]
    return j

print('OK')


import sys
import os

from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)

def assert_cannot_submit(j):
    from GangaCore.GPIDev.Lib.Job.Job import JobError
    try:
        j.submit()
        assert False, 'submit() should raise JobError'
    except JobError:
        pass  

def assert_cannot_kill(j):
    from GangaCore.GPIDev.Lib.Job.Job import JobError    
    try:
        j.kill()
        assert False, 'kill() should raise JobError'
    except JobError:
        pass  

def sleep_until_state(j, timeout=None, state='completed', break_states=None, sleep_period=1, verbose=False):
    '''
    Wait until the job reaches the specified state
    Returns:
     True: if the state has been reached in the given timeout period
     False: timeout occured or a break state has been reached
    If break_states is specified, the call terminates when job enters in one of these state, returning False
    '''

    from GangaCore.GPIDev.Base.Proxy import stripProxy
    j = stripProxy(j)
    if j.master is not None:
        j = j.master

    if timeout is None:
        timeout = config['timeout']
        
    from time import sleep
    from GangaCore.Core import monitoring_component
    from GangaCore.Core.GangaRepository import getRegistryProxy
    
    jobs = getRegistryProxy('jobs')

    current_status = None
    while j.status != state and timeout > 0:
        if not monitoring_component.isEnabled():
            monitoring_component.runMonitoring(jobs=jobs.select(j.id,j.id))
        else:
            monitoring_component.alive = True
            monitoring_component.enabled = True
            monitoring_component.steps = -1
            monitoring_component.__updateTimeStamp = 0
            monitoring_component.__sleepCounter = -0.5
        if verbose and j.status != current_status:
            logger.info("Job %s: status = %s" % (str(j.id), str(j.status)))
        if current_status is None:
            current_status = j.status
        if type(break_states) == type([]) and j.status in break_states:
            logger.info("Job finished with status: %s" % j.status )
            return False
        sleep(sleep_period)
        timeout -= sleep_period
        logger.debug("Status: %s" % j.status)
    logger.info("Job finished with status: %s" % j.status )
    logger.info("Timeout: %s" % str(timeout))
    try:
        j._getRegistry().updateLocksNow()
    except:
        pass
    return j.status == state

def sleep_until_completed(j, timeout=None, sleep_period=1, verbose=False):
    return sleep_until_state(j, timeout, 'completed', ['new','killed','failed','unknown','removed'], verbose=verbose, sleep_period=sleep_period)

def is_job_state(j, states=None, break_states=None):
    if states is None:
        states = ['completed']
    #Allow the completed state to be a list of status.
    if j.status in states:
        return True
    else:
        if break_states:
            if type(break_states) == type([]):
                assert (j.status not in break_states), 'Job did not complete (Status = %s)' % j.status
                return False
        else:
            return False
            

def is_job_finished(j):
    ''' Once the job status has reached the final status, then True. '''
    return is_job_state(j, ['completed','new','killed','failed','unknown','removed'])

def is_job_completed(j):
    return is_job_state(j, ['completed'], ['new','killed','failed','unknown','removed']) 
        
def file_contains(filename,string):
    f = open(filename, 'r')
    return f.read().find(string) != -1

def write_file(filename,content):
    """ Open,write and close the file descriptor"""
    f = open(filename,'w')
    try: return f.write(content)
    finally: f.close()

def read_file(filename):
    """ read the file, and safely close the file at the end"""
    f = open(filename)
    try: return "\n%s\n" % f.read()
    finally: f.close()

import unittest
failureException = unittest.TestCase.failureException

try:
    from GangaCore.Utility.Config import getConfig
    config = getConfig('TestingFramework')
except: # if we are outside Ganga, use a simple dict
    config={}


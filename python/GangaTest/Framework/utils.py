import sys
import os

def assert_cannot_submit(j):
    from Ganga.GPI import JobError
    try:
        j.submit()
        assert False, 'submit() should raise JobError'
    except JobError:
        pass  

def assert_cannot_kill(j):
    from Ganga.GPI import JobError    
    try:
        j.kill()
        assert False, 'kill() should raise JobError'
    except JobError:
        pass  

def sleep_until_state(j,timeout=None,state='completed', break_states=None,sleep_period=10, verbose=False):
    '''
    Wait until the job reaches the specified state
    Returns:
     True: if the state has been reached in the given timeout period
     False: timeout occured or a break state has been reached
    If break_states is specified, the call terminates when job enters in one of these state, returning False
    '''
    if timeout is None:
        timeout = config['timeout']
        
    from time import sleep
    
    current_status = None
    while j.status != state and timeout > 0:
        if verbose and j.status != current_status:    
            print j.id,j.status
        if current_status is None:
            current_status = j.status
        if type(break_states) == type([]) and j.status in break_states:
            return False
        sleep(sleep_period)
        timeout -= sleep_period
    return j.status == state

def sleep_until_completed(j,timeout=None):
    return sleep_until_state (j,timeout,'completed',['new','killed','failed','unknown','removed'])
        
def file_contains(filename,string):
    return file(filename).read().find(string) != -1

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
    from Ganga.Utility.Config import getConfig
    config = getConfig('TestingFramework')
except: # if we are outside Ganga, use a simple dict
    config={}


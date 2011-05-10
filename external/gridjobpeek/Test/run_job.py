#!/usr/bin/python

'''Executable which writes log data to a file and runs the grid job peek Peeker software to allow the log file to be monitored.'''

import sys
import time

from Peeker import Peeker
from log_file_writer import log_file_writer

#get input parameters
run_time = float(sys.argv[1])
average_interval = float(sys.argv[2])
average_size = float(sys.argv[3])
log_file = sys.argv[4]
job_id = sys.argv[5]
info_file = sys.argv[6]

#set the writer_run_time to less than the peeker run time so that the peeker has a chance to get all the data
writer_run_time = 0.85*run_time

#-------------------------------------------------------------------------#
#write lines of text to the log file
writer = log_file_writer(writer_run_time, average_interval,
                         average_size, log_file)
writer.start()

#-------------------------------------------------------------------------#
#run the peeker software

if len(sys.argv) > 7:
    config_file = sys.argv[7]
    peeker = Peeker(job_id, info_file, config_file)
else:
    peeker = Peeker(job_id, info_file)
    
peeker.start()
time.sleep(run_time)
peeker.stop()

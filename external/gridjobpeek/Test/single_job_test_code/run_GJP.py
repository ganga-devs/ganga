#!/usr/bin/python

'''Executable which runs the log_writer software to write lines of text to a dummy file, the peeker software to allow this file to be monitored and the client software to monitor the log file created'''

import sys
import time

from Peeker import Peeker
from Client_thread import Client
from log_file_writer import log_file_writer

#get input parameters
log_file = sys.argv[1]
follow = sys.argv[2]
lines = sys.argv[3]
output_file = sys.argv[4]
job_id = sys.argv[5]
info_file = sys.argv[6]
run_time = float(sys.argv[7])
average_interval = float(sys.argv[8])
average_size = float(sys.argv[9])

#writer_run_time<peeker_run_time so peeker has a chance to get all the data
writer_run_time = 0.90 * run_time

#--------------------------------------------------------------------------#
#run the GjpClient software
client = Client(log_file, follow, lines, output_file, job_id, info_file)
client.start()

#-------------------------------------------------------------------------#
#write lines of text to the log file
writer = log_file_writer(writer_run_time, average_interval,
                       average_size, log_file)
writer.start()

#-------------------------------------------------------------------------#
#run the GjpJob software
peeker = Peeker(job_id, info_file)
peeker.start()
time.sleep(run_time)
peeker.stop()

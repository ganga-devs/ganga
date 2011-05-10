#!/usr/bin/python

'''Executable which runs the client software to monitor the log file created by the peeker software'''

import sys
import time

from Client import Client

#get input parameters
log_file = sys.argv[1]
follow = sys.argv[2]
lines = sys.argv[3]
output_file = sys.argv[4]
job_id = sys.argv[5]
info_file = sys.argv[6]


#--------------------------------------------------------------------------#
#run the GjpClient software
client = Client(log_file, follow, lines, output_file, job_id, info_file)
client.tail()


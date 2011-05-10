
'''Grid_tail.py
Script used to run the grid job peek client software.'''

import time
from Client import Client
from optparse import OptionParser

p=OptionParser()

#set default input parameters
p.set_defaults(follow=False, number_lines=10, log_file='log_file',
               output_file='output_file',
               scalability_data_file='info_file')

#get input values from the command line options
p.add_option('-f', '--follow', action='store_true', dest='follow',
              help='log file is followed if option is seen')

p.add_option('-n', '--lines', type='int', dest='number_lines',
             help='number of lines to retrieve from bottom of log file')

p.add_option('-L', '--log_file', type='string', dest='log_file',
             help='log file to be monitored')

p.add_option('-o', '--output_file', type='string', dest='output_file',
             help='file to which log data is written')

p.add_option('-d', '--scalability_data_file', type='string',
             dest='scalability_data_file',
             help='file to which the scalability data is written')

(options, args) = p.parse_args()

#run client 
client = Client(options.log_file, options.follow, options.number_lines,
            options.output_file, args[0], options.scalability_data_file)
client.tail()



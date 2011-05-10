'''Class that contains functions to write lines of random text to a log file.  The average length of the line and average interval between lines being written can be varied'''

import sys
import time
import string
import random

from threading import Thread


class log_file_writer(Thread):

    def __init__(self, writer_run_time, average_interval,
                 average_size, log_file):
        
        Thread.__init__(self)
        self.total_time = float(writer_run_time)
        self.average_interval = average_interval
        self.average_size = average_size
        self.log_file = log_file


    def run(self):
        '''Function that writes log data to a dummy log_file.  Function will run for total_time and write strings of average_size with an frequency governed by average_interval.'''

        end_time = (time.time() + self.total_time)

        while time.time() < end_time:
            f = open(self.log_file,'a')

            #calculate time interval
            min_interval = 0.7 * self.average_interval
            max_interval = 1.3 * self.average_interval
            time_interval = random.uniform(min_interval, max_interval)
 
            #create line from combination of letters, numbers, punctation
            min_size = int(0.7 * self.average_size)
            max_size = int(1.3 * self.average_size)
            line = (''.join([random.choice(string.letters + string.digits +
                   string.punctuation)
                   for j in range(random.randint(min_size, max_size))]) + '\n')

            #write line to log file
            f.write(line)
            f.close()
            
            time.sleep(time_interval)
        

if __name__=='__main__':

    #get input parameters
    run_time = float(sys.argv[1])
    average_interval = float(sys.argv[2])
    average_size = sys.argv[3]
    log_file = sys.argv[4]

    #run program
    L = log_file_writer(run_time, average_interval, average_size, log_file)
    L.start()



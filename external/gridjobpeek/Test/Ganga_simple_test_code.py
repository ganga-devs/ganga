'''Program that handles the submission of the test jobs via Ganga for the functionality testing of grid job peek.'''


# Functionality test:
# Submit i jobs which will both write lines of text to a log file and run the peeker software.
# Jobs are monitored by running the client software from command line with:

# python grid_tail.py -f -n 3 -L 'log_file_***' -o 'output_file_***' -d 'info_file' '***'
# where *** is the Ganga job number

#--------------------------------------------------------------------------#
path_name = '/afs/cern.ch/user/a/awilcock/Test/'

#how long job will run for
run_time = '1200'
#how frequently log data will be written to log file
average_interval = '1'
#average number of characters in a line of log data
average_size = '80'
#number of jobs
num_jobs = 1
#file that the scalability data will be written to
info_file = path_name + 'info_file'

#--------------------------------------------------------------------------#

for i in range(num_jobs):  
    peeker = Job()
    
    #get job ID
    job_id = str(peeker.id)

    #create name of log file
    log_file = path_name + 'log_file_' + job_id
    
    ARGS = [run_time, average_interval, average_size,
            log_file, job_id, info_file]
    
    peeker.application = Executable(exe=File(path_name+'run_job.py'),
                                    args=ARGS)

    #specify input files
    peeker.inputsandbox = [File(path_name+'log_file_writer.py'),
                           File(path_name+'Peeker.py'),
                           File(path_name+'get_log_data.py'),
                           File(path_name+'stomp/cli.py'),
                           File(path_name+'stomp/exception.py'),
                           File(path_name+'stomp/__init__.py'),
                           File(path_name+'stomp/listener.py'),
                           File(path_name+'stomp/stomp.py'),
                           File(path_name+'stomp/utils.py')]

    #peeker.backend = LSF()
    peeker.submit()


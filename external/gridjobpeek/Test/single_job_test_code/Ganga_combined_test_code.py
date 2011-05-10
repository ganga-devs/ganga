'''Program that handles the submission of the test jobs via Ganga for the scalability testing of grid job peek.'''


#Scalability test
#Submit i jobs
# - one part of the job creates a log file and runs peeker
# - one part of the job runs client to monitor the log file

#--------------------------------------------------------------------------#

path_name = '/afs/cern.ch/user/a/awilcock/gridjobpeek/Test/'

#how long job will run for
run_time = '1200'
#how frequently log data will be written to log file
average_interval = '1'
#average number of characters in a line of log data
average_size = '50'
#number of pairs of jobs
num_jobs = 1

#define action
follow = 'True'
number_lines = '-1'

#--------------------------------------------------------------------------#
test = Job()
test.submit()
first_job = test.id+1

for i in range(num_jobs):
    
    #get job id 
    job_id = str(first_job+(i))
    
    j = Job()

    ARGS = ['log_file', follow, number_lines, 'output_file', job_id,
            'info_file', run_time, average_interval, average_size]
    
    j.application = Executable(exe=File(path_name+'run_GJP.py'), args=ARGS)
    
    #specify input files
    j.inputsandbox = [File(path_name+'Client_thread.py'),
                      File(path_name+'log_file_writer.py'),
                      File(path_name+'Peeker.py'),
                      File(path_name+'get_log_data.py'),
                      File(path_name+'stomp/cli.py'),
                      File(path_name+'stomp/exception.py'),
                      File(path_name+'stomp/__init__.py'),
                      File(path_name+'stomp/listener.py'),
                      File(path_name+'stomp/stomp.py'),
                      File(path_name+'stomp/utils.py')]
    
    j.outputsandbox = ['output_file','info_file', 'log_file']
    j.backend = LCG()
        
    j.submit()



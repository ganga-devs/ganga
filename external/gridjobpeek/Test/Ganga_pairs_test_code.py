'''Program that handles the submission of the test jobs via Ganga for the scalability testing of grid job peek.'''


#Scalability test
#Submit i  pairs of jobs
# - one job (peeker) creates a log file and runs peeker software
# - one job (client) runs client software to monitor the log file

#--------------------------------------------------------------------------#
import md5
import time

path_name = '/afs/cern.ch/user/a/awilcock/Test/'

#how long job will run for
run_time = '60'
#how frequently log data will be written to log file
average_interval = '1'
#average number of characters in a line of log data
average_size = '80'
#number of pairs of jobs
num_pairs = 1
#file that the scalability data will be written to
info_file = path_name + 'info_file'
#define action
follow = 'True'
number_lines = '-1'

#--------------------------------------------------------------------------#

#find the ID of the first job
test = Job()
test.submit()
first_job = test.id + 1

for i in range(num_pairs):
    
    #assign a pair ID
    pair_id = str(first_job + (2 * i))

    #create a pair of jobs
    client = Job(name='client_'+pair_id)
    peeker = Job(name='peeker_'+pair_id)
    
    #create names for log and output files
    log_file = path_name + 'log_file_' + pair_id
    output_file = path_name + 'output_file_' + pair_id
    
    client_args = [log_file, follow, number_lines,
                   output_file, pair_id, info_file]
    
    client.application = Executable(exe= File(path_name+'run_client.py'),
                                    args=client_args)
    
    #specify input files
    client.inputsandbox = [File(path_name+'Client.py'),
                           File(path_name+'stomp/cli.py'),
                           File(path_name+'stomp/exception.py'),
                           File(path_name+'stomp/__init__.py'),
                           File(path_name+'stomp/listener.py'),
                           File(path_name+'stomp/stomp.py'),
                           File(path_name+'stomp/utils.py')]

    client.backend = LSF()
    client.submit()

    #if the client is still running - submit the peeker
    if not jobs(pair_id).status == 'completed':

        job_args = [run_time, average_interval, average_size, log_file,
                    pair_id, info_file]

        peeker.application = Executable(exe=File(path_name+'run_job.py'),
                                        args=job_args)

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

        peeker.backend = LSF()
        peeker.submit()

       

#--------------------------------------------------------------------------#
#compare output file from the client software to log file created by the peeker

compared_files = {}
j = 1

while len(compared_files) < num_pairs:
    
    client_id = str(first_job + (j - 1))
    pair_id = client_id
    peeker_id = str(first_job + j)
    
    msg = 'Test '+pair_id+' failed: unknown reason'
    
    if (jobs(client_id).status == 'completed'
        and jobs(peeker_id).status == 'completed'
        and not compared_files.has_key(pair_id)):
        
        #open log file and output file
        try:
            log_file = open(path_name + 'log_file_' + pair_id, 'r')
            
            try:
                output_file = open(path_name + 'output_file_' + pair_id, 'r')

                #get content of files
                log_data = log_file.readlines()
                output_data = output_file.readlines()

                log_check = md5.new(str(log_data)).digest()
                output_check = md5.new(str(output_data)).digest()

                #check if log file matches output file
                if log_check == output_check:
                    msg = 'Test ' + pair_id + ' passed'

                elif len(log_data) > len(output_data):

                    #check output file hasn't just missed the last few lines
                    reduced_log_data = log_data[0:len(output_data)]
                    reduced_log_data_check = (md5.new(str(reduced_log_data))
                                              .digest())

                    if reduced_log_data_check == output_check:
                        msg = ('Test ' + pair_id +
                ' failed:log file missing the last %d lines of the output file'
                               % int(len(log_data)-len(output_data)))

            except IOError:
                msg = ('Test ' + pair_id + ' failed: output file '
                       + pair_id + ' not_found')
        except IOError:
            msg = ('Test ' + pair_id + ' failed: log file '
                   + pair_id + ' not found')

        #write the test results to a file
        f = open(path_name+'test_result','a')
        f.write(msg+'\n')
        f.close()
        
        compared_files[pair_id] = 1
        
    #move ontot the next pair
    j += 2

    #once been through all pairs - go back to the beginning
    if j > (num_pairs*2):
        j = 1


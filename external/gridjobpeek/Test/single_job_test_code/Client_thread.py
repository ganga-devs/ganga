'''Grid Job Peek client

Contains the client functions for the grid job peek software.

Contains functionality to:
- subscribe to a topic (CCD) and wait for a request messages from the peeker
- compare job ID in request message to job ID in user defined action
- send a response message, containing the action, to the destination specified in the request message (JCD)
- subscribe to a topic (CSD)and wait to receive log data from the peeker

version of code used when running the client and the peeker as a single job on the grid.
'''

import os
import sys
import time
import random
import logging
from threading import Thread

import stomp

#---------------------------------------------------------------------------#

class ClientListener(object):
    '''Class containing functions that
        - display the status of the message exchange process
        - define the programs response to receiving messages from the peeker'''
    
    def __init__(self, client):
        self.client = client
        self.action_id = str(random.random())
        self.CSD_subscription = False
         
    def on_connecting(self, host_and_port):
        print 'connecting...'

    def on_disconnected(self):
        print "lost connection"

    def on_message(self, headers, body):
        '''Function that initiates the appropriate response of the program based on the type of message received '''
        
        self.__print_async("MESSAGE", headers, body)
        
        try:  
            if headers['message_type'] == 'request':
                self.__request_message(body, headers)
               
            elif headers['message_type'] == 'stop':
                    self.__stop_message(body, headers)

            elif headers['message_type'] == 'log data':
                print 'log data msg'
                self.__log_data_message(body, headers)
                
        except Exception, err:
            print err

    def on_error(self, headers, body):
        self.__print_async("ERROR", headers, body)     

    def on_receipt(self, headers, body):
        self.__print_async("RECEIPT", headers, body)

    def on_connected(self, headers, body):
        self.__print_async("CONNECTED", headers, body)

    def __print_async(self, frame_type, headers, body):
        print "\r  \r",
        print frame_type

        for header_key in headers.keys():
            print '%s: %s' % (header_key, headers[header_key])
        print
        print body
        print '\n'
        print '> ',
        sys.stdout.flush()


    def __request_message(self, msg_body, msg_headers):
        '''Function that is called on receipt of a request message from the peeker.  Function will,
         - compare job IDs in request message and user defined action
         - send reply message containing the action to the JCD
         - subscribe to the CSD and wait to receive log data from the peeker'''
            
        action = [self.client.job_id, self.client.follow,
                self.client.lines, self.client.log_file]

        #compare job IDs in request message and action 
        polling_job_id = str(msg_headers['job_id'])
        
        if self.client.job_id == polling_job_id:
        
            #get name of the JCD from peeker request message
            JCD = msg_headers['reply-to']

            #send reply message to JCD
            JCD_headers = {'reply-to':self.client.CSD,
                           'message_type':'response',
                           'correlation_id':self.action_id}
            self.client.conn.send(action, destination=JCD, headers=JCD_headers)
            
            #listen for log data messages from the peeker
            if self.CSD_subscription == False:
                self.client.conn.subscribe(destination=self.client.CSD)
                self.CSD_subscription = True
            
            #store scalability test data and the start time
            self.client.scalability_data.append(['Request message ',
                        int(msg_headers['timestamp']), len(msg_body)])
            self.client.start_time = float(msg_headers['start_time'])


    def __log_data_message(self, msg_body, msg_headers):
        '''Function that is called on receipt of a message from the client that contains log data. Function will,       
         - write log data to file
         - update the action'''

        #check log data msg is meant for this client
        if msg_headers['correlation_id'] == self.action_id:

            #retrieve log data and write it to specified output file
            log_data = msg_body
            if not log_data == '':
                output_file = open(self.client.output_file, 'a')
                output_file.write(log_data)
                output_file.close()

            #update action and stop client if no action remains
            self.client.lines = 0
            if self.client.follow == False:
                self.client.stop()
                
            #store scalability test data
            self.client.scalability_data.append(['Log data message',
                          int(msg_headers['timestamp']), len(msg_body)])


    def __stop_message(self, msg_body, msg_headers):
        '''Function that is called on receipt of a message from the peeker indicating the the job has finished running.  Function will.
        - shut down client'''

        #check message come from the job being peeked at
        polling_job_id = str(msg_headers['job_id'])
        if polling_job_id == self.client.job_id:
            
            #store scalability test data
            self.client.scalability_data.append(['Stop message    ',
                                 int(msg_headers['timestamp']), len(msg_body)])

            #unsubscribe from all destinations
            self.client.conn.unsubscribe(destination=self.client.CCD)
            self.client.conn.unsubscribe(destination=self.client.CSD)

            #write any scalability test data to the scalability data file
            try:
                self.client.write_scalability_data()
            except Exception, err:
                f=open(self.client.scalability_data_file, 'a')
                f.write('Client ' + self.job_id + ':\nNo time data \n\n')
                f.close()
                
            #call the clients stop function
            self.client.stop()


#---------------------------------------------------------------------------#


class Client(Thread, ClientListener):
    '''Class containing the main control functions for the grid job peek client.'''

    def __init__(self, log_file, follow, lines, output_file,
                 job_id, scalability_data_file):
        Thread.__init__(self)
        self.should_stop = False
        self.log_file = str(log_file)
        self.follow = bool(follow)
        self.lines = int(lines)
        self.output_file = str(output_file)
        self.job_id = str(job_id)
        self.username = str(os.getuid())
        self.host = 'gridmsg002.cern.ch'            
        self.port = 6163
        self.CCD = '/topic/' + self.username + '_CCD/'
        self.CSD = '/topic/' + self.username + '_CSD/'
        self.scalability_data_file = scalability_data_file
        self.scalability_data = []
        self.start_time = 0
  
    def run(self):
        '''Function that runs the grid job peek client'''         
        
        conn = stomp.Connection([(self.host,self.port)])
        self.conn = conn
        self.listener = ClientListener(self)
        conn.set_listener('ClientListener', self.listener)
        conn.start()
        conn.connect()
        
        try:
            try:
                #listen for a request message from the peeker
                conn.subscribe(destination=self.CCD)

                while not self.should_stop:
                    #run until a stop command is received from user or peeker
                    pass
                
            except KeyboardInterrupt:
                print 'Keyboard Interrupt'
                self.stop()
                
        finally:
            self.stop()
        

    def stop(self):
        '''Function that stops the grid job peek client.'''
        self.should_stop = True
        self.conn.disconnect
        sys.exit()

    def write_scalability_data(self):
        '''Function that writes the scalability test data to file'''
        
        #calc the time messages were received relative to the start time
        relative_times = []
        for i in range(len(self.scalability_data)):
            relative_times.append(self.scalability_data[i][1] -
                             float(self.start_time))


        #if any scalability data has been recoreded write it to file
        f = open(self.scalability_data_file, 'a')
        f.write('Client ' + self.job_id + ':\n')
        if not len(self.scalability_data)<1:
            f.write('Message type  \t   Time received   Delta t   Number bytes\n')
            for j in range(len(self.scalability_data)): 
                f.write(str(self.scalability_data[j][0]) + '   ' +
                        str(self.scalability_data[j][1]) + '   ' +
                        '+' + str(relative_times[j])+ '        ' +
                        str(self.scalability_data[j][2]) + '\n')
        else:
            f.write('No time data')
            
        f.write('\n')
        f.close()

if __name__=='__main__':
    
    logging.basicConfig()
    
    #get inputs
    log_file = sys.argv[1]
    follow = sys.argv[2]
    lines = sys.argv[3]
    output_file = sys.argv[4]
    job_id = sys.argv[5]
    info_file = sys.argv[6]

    #run client
    client = Client(log_file, follow, lines, output_file, job_id, info_file)
    client.run()

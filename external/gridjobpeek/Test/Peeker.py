
''' Grid Job Peek Peeker

Contains the job wrapper functions for the grid job peek software.

Contains functionality to:
- set up a temporary queue (JCD)
- send a message to a control destination (CCD) containing the jobs id and a reply-to destination of the JCD
- listen to the JCD for a response for a set time
- if response is received:
     perform the action contained in the response
     send the log data to a streaming destination (CSD)
- if no response is received:
     become inactive and remain so for a set time
- send stop message to the CCD once the job has finished running
'''

import os
import sys
import time
import logging
import ConfigParser
from threading import Thread

import stomp
from get_log_data import tail_file
    
#-------------------------------------------------------------------------#

class PeekerListener(object):
    '''Class containing functions that
        - display the status of the message exchange process
        - define the programs response to receiving messages from the client'''
    
    def __init__(self, peeker):
        self.peeker = peeker
        self.msg_type = ''        

    def on_connecting(self, host_and_port):
        print 'connecting...'

    def on_disconnected(self):
        print "lost connection"

    def on_message(self, headers, body):
        '''Function that initiates the appropriate response of the program based on the type of message received '''
        
        #self.__print_async("MESSAGE", headers, body)
        
        if headers['message_type'] == 'JCD name':  
            try:
                self.__JCD_name(headers, body)

                #if this is the first message received - set the start time 
                if self.peeker.start_time == 0:
                    self.peeker.start_time = headers['timestamp']
            except Exception, err:
                self.msg_type = ''
            
        if headers['message_type'] == 'response':
            try:
                self.__response_message(headers, body)
            except Exception, err:
                pass
         
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

        
    def __JCD_name(self, msg_headers, msg_body):
        '''Function that is called on receipt of a message containing the name of the JCD.  Function will,
         - get the name of the JCD from the message
         - send a request message to the CCD'''        
     
        #get the name of the JCD 
        JCD = msg_headers['destination']
        
        #set msg to expire once peeker has finished waiting for client response
        expiration_time = int((self.peeker.reply_time + time.time()) * 1000)

        #send a request message to the CCD
        CCD_headers = {'expires': expiration_time,
                       'reply-to':JCD,
                       'message_type': 'request',
                       'job_id':self.peeker.job_id,
                       'start_time':self.peeker.start_time}
        self.peeker.conn.send(destination=self.peeker.CCD, headers=CCD_headers)

        #store scalability test data
        self.peeker.scalability_data.append(['JCD name message',
                                 int(msg_headers['timestamp']), len(msg_body)])
        #update the message type
        self.msg_type = msg_headers['message_type']
                                      

    def __response_message(self, msg_headers, msg_body):
        '''Function that is called on receipt of a response message from the client. Function will,       
         - get action contained in the response message
         - perform action
         - send the resulting log data to the CSD'''

        time.sleep(0.1)
        
        #get action contained in the response message from the client
        action = eval(msg_body)
        follow = action[1]
        lines = action[2]
        log_file = action[3]  

        #get reply-to destination and correlation id from response message
        self.peeker.CSD = msg_headers['reply-to']
        action_id = msg_headers['correlation_id']

        #perform action 
        log_data, self.peeker.current_tails = tail_file(follow, lines,
                                                    log_file, action_id,
                                                    self.peeker.current_tails,
                                                    self.peeker.block_size,
                                                    self.peeker.msg_size_limit)

        #send log data message to the CSD
        CSD_headers = {'message_type':'log data', 'correlation_id':action_id}
        self.peeker.conn.send(log_data, destination=self.peeker.CSD,
                              headers=CSD_headers)

        #store scalability test data
        self.peeker.scalability_data.append(['Response message',
                                  int(msg_headers['timestamp']),len(msg_body)])
        #update the message type
        self.msg_type = msg_headers['message_type']


#--------------------------------------------------------------------------#


class Peeker(Thread, PeekerListener):
    '''Class containing the main control functions for the grid job peek job wrapper.'''

    def __init__(self, job_id, scalability_data_file, config_file=None):
        
        Thread.__init__(self)
        self.setDaemon(True)
        self.should_stop = False
        self.job_id = job_id
        self.scalability_data_file = scalability_data_file
        self.start_time = 0
        self.scalability_data = []

        #get remainder of the inputs from the config file
        if config_file:
            config = ConfigParser.ConfigParser()
            config.read(config_file)

            self.block_size = config.getint('Log data settings', 'block_size')
            self.msg_size_limit = config.getint('Log data settings',
                                              'msg_size_limit')
            self.reply_time = config.getfloat('Timing settings', 'reply_time')
            self.inactive_time = config.getfloat('Timing settings',
                                               'inactive_time')
            self.host = config.get('Broker settings', 'host')
            self.port = config.getint('Broker settings', 'port')
            self.CCD = config.get('Broker settings', 'CCD')

        #or if no config file is given - use default values
        else:
            self.block_size = 1024
            self.msg_size_limit = 1000000
            self.reply_time = 5.0
            self.inactive_time = 300.0
            self.host = 'gridmsg001.cern.ch'
            self.port = 6163
            username = str(os.getuid())
            self.CCD = '/topic/' + username + '_CCD/'

    def run(self):
        '''Function that runs the grid job peek job wrapper'''
        
        try:
            self.current_tails = {}
            while not self.should_stop:
                
                conn = stomp.Connection([(self.host,self.port)])
                self.conn = conn
                self.listener = PeekerListener(self)
                conn.set_listener('PeekerListener', self.listener)
                conn.start()
                conn.connect()

                #set up a temporary job control destination and subscribe to it
                JCD = '/temp-queue/JCD/'
                conn.subscribe(destination=JCD)

                #sent message to JCD and wait to receive it
                JCD_headers = {'persistent':'true', 'message_type':'JCD name'}
                conn.send(destination=JCD, headers=JCD_headers)
                while not self.listener.msg_type == 'JCD name':
                    time.sleep(0.1)

                #listen for response messages from clients
                end_time = time.time() + self.reply_time
                response_received = False
                
                while time.time()<end_time:
                    if self.listener.msg_type == 'response':
                        #reset msg_type to allow for multiple response messages
                        self.listener.msg_type = ''
                        response_received = True  
                    else:
                        time.sleep(0.1)
                                     
                conn.unsubscribe(destination=JCD)

                #is no clients respond peeker becomes inactive
                if not response_received:
                    time.sleep(self.inactive_time)
                    
                self.listener.msg_type = ''    
                conn.disconnect()

        finally:
            conn.disconnect()
            self.stop()


    def stop(self):
        '''Function that stops the grid jobe peek job wrapper.'''
        
        self.should_stop = True

        #write any scalability test data to the scalability data file
        try:
            self.__write_scalability_data()
        except Exception, err:
            f = open(self.scalability_data_file, 'a')
            f.write('Peeker ' + self.job_id + ':\n')
            f.write('No time data\n')
            f.close()

        #send msg to CCD to say peeker has stopped running
        conn = stomp.Connection([(self.host, self.port)])
        self.conn = conn
        self.listener = PeekerListener(self)
        conn.set_listener('PeekerListener', self.listener)
        conn.start()
        conn.connect()
        CCD_headers = {'message_type':'stop', 'job_id': self.job_id}
        conn.send(destination=self.CCD, headers=CCD_headers)
        conn.disconnect()

    def __write_scalability_data(self):
        '''Function that writes the scalability test data to file'''

        #calc the time messages were received relative to the start time
        relative_times = []
        for i in range(len(self.scalability_data)):
            relative_times.append(self.scalability_data[i][1] -
                                  float(self.start_time))
        
        #write scalability data to file
        f = open(self.scalability_data_file, 'a')
        f.write('Peeker ' + self.job_id + ':\n')
        f.write('Message type  \t   Time received   Delta t   Number bytes\n')
        for j in range(len(self.scalability_data)):
            f.write(str(self.scalability_data[j][0]) + '   ' +
                    str(self.scalability_data[j][1]) + '   ' +
                    '+' + str(relative_times[j])+ '        ' +
                    str(self.scalability_data[j][2])+'\n')

        f.write('\n')
        f.close()
        
if __name__ == '__main__':
    
    logging.basicConfig()
    
    #get input parameters
    run_time = float(sys.argv[1])
    job_id = sys.argv[2]
    info_file = sys.argv[3]

    if len(sys.argv) > 4:
        config_file = sys.argv[4]
        peeker = Peeker(job_id, info_file, config_file)
    else:
        peeker = Peeker(job_id, info_file)

    #run peeker
    peeker.start()
    time.sleep(run_time)
    peeker.stop()
    


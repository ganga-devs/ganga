#! /bin/env python

"""
WARNING: urllib2 does not validate SSL certificates before python 3.2.2.

This is insecure (the USERNAME and PASSWORD could theoretically be snooped by a 
man-in-the-middle attack).

@TODO add cafile parameter when python upgraded to 3.2.2

-----

bash-3.2$ ./post_status.py -h
usage: ./post_status.py [options] MONDIR RUN SUBRUN TRIGTYPE STAGE
       ./post_status.py --site=scinet --result=1 --time=1577 --read=5062 --written=310 production004/B/rdp/ND280/00005000_00005999 5012 6 spill reco
       ./post_status.py -h
       
       Record metadata about a processing job in the processingstatus db.
       
       Intended to be called by check_mon_info.pl.

       If a server problem occurs, this script retries up to MAX_RETRIES times.

       EXIT CODES

       0 - Request succeeded
       1 - Unknown problem
       2 - Failed to parse command line (detected by this script)
       3 - Command line argument(s) invalid or incorrect type (detected by the database server)
       4 - Server problem (http response code != 201)

arguments:
       MONDIR - container for a set of metadata records
       should be initialized following the Production Directory Structure
       http://www.t2k.org/nd280/datacomp/howtoaccessdata/directorystructure

       RUN - integer                                               --| Composite Primary 
       SUBRUN - integer                                              | Key that
       TRIGTYPE - string                                             | identifies a
       https://neut00.triumf.ca/t2k/processing/status/trigtypes      | processing job
       STAGE - string                                                | within a mondir 
       https://neut00.triumf.ca/t2k/processing/status/stages       --|

options:
  -h, --help         show this help message and exit
  --site=SITE        Site (string)
  --result=RESULT    Result (integer) previously registered at https://neut00.
                     triumf.ca/t2k/processing/status/result_codes
  --time=TIME        Wallclock time in seconds (integer)
  --read=READ        Events read from input file (integer)
  --written=WRITTEN  Events written to output file (integer)

-----

It would be better to communicate through message queues, rather than 
posting via HTTP directly to the application (this would make the posting of 
statuses more reliable, scalable, flexible and require less custom code).

However, this would require additional infrastructure:

 * an AMQP message broker (such as RabbitMQ or better yet, ZeroMQ)
 * an AMQP library (such as Carrot)
 * a data serialization format (such as json or msgpack)

These dependencies would make the system harder to maintain.
"""

#APP_ROOT='http://neut07.triumf.ca:4000/'
#APP_ROOT='https://neut00.triumf.ca/t2k/processing/status/'
APP_ROOT='https://nd280web.nd280.org/t2k/processing/status/'

USERNAME='procstatupdater'
PASSWORD='2aT0n8Ra'

MAX_RETRIES = 20

#####

import optparse

import urllib.parse, urllib.request, urllib.parse, urllib.error, urllib.request, urllib.error, urllib.parse

import random, time

import sys

#import pdb

try:
    # for python 2.6+
    import json
except ImportError:
    # for python < 2.6
    class json:
        @staticmethod
        def dumps(obj):
            if obj.__class__ != dict:
                raise Exception("Only dict serialization is implemented")
            o = []
            for (k,v) in list(obj.items()):
                if v is None:
                    o.append( '"%s": null' % (k, ) )
                elif v.__class__ == str:
                    o.append( '"%s": "%s"' % (k, v) )
                elif v.__class__ in [int, float]:
                    o.append( '"%s": %s' % (k, v) )
                else:
                    o.append( '"%s": %s' % (k, json.dumps(v)) )
            return "{%s}" % (', '.join(o), )

#####

def record(mondir, job, attributes):

    #blech - we have to subclass to handle responses with 201 status code
    class CustomHTTPErrorProcessor(urllib2.HTTPErrorProcessor):
        handler_order = 1000  # after all other processing
        def http_response(self, request, response):
            code, msg, hdrs = response.code, response.msg, response.info()
            if code != 201:
                response = self.parent.error('http', request, response, code, msg, hdrs)
            return response
        https_response = http_response

    query_string = urllib.parse.urlencode(job)
    mondir_url = urllib.parse.urljoin(APP_ROOT, mondir) + '?' + query_string
    attributes_json = json.dumps(attributes)

    print("POST %s\n%s" % (mondir_url, attributes_json))

    # build auth handler
    pswd_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    pswd_mgr.add_password(None, APP_ROOT, USERNAME, PASSWORD)
    auth_handler = urllib.request.HTTPBasicAuthHandler(pswd_mgr)

    # build opener
    opener = urllib.request.build_opener(auth_handler, CustomHTTPErrorProcessor())

    attempt = 0	
    while True:
        try:
            response = opener.open( urllib.request.Request(mondir_url, attributes_json, {'Accept':'text/plain'}) )
            # (blech)
	    #pdb.set_trace()
            #(Pdb) dir(response)
            #['__doc__', '__init__', '__iter__', '__module__', '__repr__', 'close', 'code', 'fileno', 'fp', 'geturl', 'headers', 'info', 'msg', 'next', 'read', 'readline', 'readlines', 'url']
            print("%s %s" % (response.code, response.msg))
            print(response.read())
            break
        except urllib.error.HTTPError as e:
            if e.code == 400:
                print(e)
                print(e.read())
                print("Command line argument(s) invalid (detected by the database server)")
                return 3
            elif e.code == 404:
                print(e)
                print(e.read())
                print("MONDIR does not exist (detected by database server)")
                return 3
            print("Error: %s %s\n%s" % ( e.code, e.msg, e.read() ))
            if attempt < MAX_RETRIES:
                sleep_time = random.random() * 10
                print("--> Retrying in %s seconds" % (sleep_time, ))
                time.sleep(sleep_time)
                attempt += 1
                continue
            else:
                raise

#####

def parse_cmdline():
    usage = """./%prog [options] MONDIR RUN SUBRUN TRIGTYPE STAGE
       ./%prog --site=scinet --result=1 --time=1577 --read=5062 --written=310 production004/B/rdp/ND280/00005000_00005999 5012 6 spill reco
       ./%prog -h
       
       Record metadata about a processing job in the processingstatus db.
       
       Intended to be called by check_mon_info.pl.

       If a server problem occurs, this script retries up to MAX_RETRIES times.

       EXIT CODES

       0 - Request succeeded
       1 - Unknown problem
       2 - Failed to parse command line (detected by this script)
       3 - Command line argument(s) invalid or incorrect type (detected by the database server)
       4 - Server problem (http response code != 201)

arguments:
       MONDIR - container for a set of metadata records
       should be initialized following the Production Directory Structure
       http://www.t2k.org/nd280/datacomp/howtoaccessdata/directorystructure

       RUN - integer                                               --| Composite Primary 
       SUBRUN - integer                                              | Key that
       TRIGTYPE - string                                             | identifies a
       https://neut00.triumf.ca/t2k/processing/status/trigtypes      | processing job
       STAGE - string                                                | within a mondir 
       https://neut00.triumf.ca/t2k/processing/status/stages       --|"""

    p = optparse.OptionParser(usage=usage)

    p.add_option("--site", type='string', help='Site (string)')
    p.add_option("--result", type='int', help='Result (integer) previously registered at https://neut00.triumf.ca/t2k/processing/status/result_codes')
    p.add_option("--time", type='int', help='Wallclock time in seconds (integer)')
    p.add_option("--read", type='int', help='Events read from input file (integer)')
    p.add_option("--written", type='int', help='Events written to output file (integer)')

    (opts, args) = p.parse_args()

    if len(args) != 5:
        p.error("incorrect number of arguments")

    job = { 'run':int(args[1]),
            'subrun':int(args[2]),
            'trigtype':args[3],
            
            'stage':args[4] }
            
    attributes = { 'site':opts.site,
                 'result':opts.result,
                 'time':opts.time,
                 'read':opts.read,
                 'written':opts.written }

    return (args[0], job, attributes)

#####

if __name__ == '__main__':
    (mondir, job, attributes) = parse_cmdline()
    
    try:
        record(mondir, job, attributes)
    except urllib.error.HTTPError as e:
        print("Error: The HTTP request failed. (%s)" % (str(e), ))
        sys.exit(4)

    sys.exit(0)

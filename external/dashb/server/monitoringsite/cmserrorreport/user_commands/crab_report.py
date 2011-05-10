import mimetypes
import urllib2
import httplib
import string
import random

uploadFileServer= "http://gangamon.cern.ch/django/errorreports/"
#uploadFileServer= "http://127.0.0.1:8000/errorreports"

def random_string (length):
        return ''.join ([random.choice (string.letters) for ii in range (length + 1)])

def encode_multipart_data (files):
        boundary = random_string (30)

        def get_content_type (filename):
                return mimetypes.guess_type (filename)[0] or 'application/octet-stream'

        def encode_file (field_name):
                filename = files [field_name]
                return ('--' + boundary,
                                'Content-Disposition: form-data; name="%s"; filename="%s"' % (field_name, filename),
                                'Content-Type: %s' % get_content_type(filename),
                                '', open (filename, 'rb').read ())
        
        lines = []
        for name in files:
                lines.extend (encode_file (name))
        lines.extend (('--%s--' % boundary, ''))
        body = '\r\n'.join (lines)

        headers = {'content-type': 'multipart/form-data; boundary=' + boundary,
                        'content-length': str (len (body))}

        return body, headers


def run_upload (server, path):

        upload_file = make_upload_file (server)
        upload_file (path)

def make_upload_file (server):

        def upload_file (path):

                #print 'Uploading %r to %r' % (path, server)

                data = {'MAX_FILE_SIZE': '3145728',
                        'sub': '',
                        'mode': 'regist'}
                files = {'file': path}

                send_post (server, files)

        return upload_file

def send_post (url, files):

        req = urllib2.Request (url)
        connection = httplib.HTTPConnection (req.get_host ())
        connection.request ('POST', req.get_selector (),
               *encode_multipart_data (files))
        response = connection.getresponse ()

        responseResult = response.read()
        #get the result from the post in a result file
        #open('/home/ivan/result.txt', 'w').write(responseResult)

        responseResult = responseResult[responseResult.find("<span id=\"download_path\""):]
        startIndex = responseResult.find("path:") + 5
        endIndex = responseResult.find("</span>")

        print 'Your error report was uploaded to ganga developers with the following URL. '
        print 'You may include this URL in your bug report or in the support email to the developers.'
        print
        print '***',responseResult[startIndex:endIndex],'***'
        print

#with metadata
#run_upload(server=uploadFileServer, path='/home/ivan/Desktop/userreport2010-07-16-11:45:24.tar.gz')
#without metadata file
#run_upload(server=uploadFileServer, path='/home/ivan/Desktop/testwithoutmeta.tar.gz')
#zip file- it is intended to be only with metadata

if __name__ == "__main__":
        import sys

        try:
                report_path = sys.argv[1]
        except IndexError:
                print "Specify an argument"
                sys.exit(-1)

        run_upload(server=uploadFileServer, path=report_path)


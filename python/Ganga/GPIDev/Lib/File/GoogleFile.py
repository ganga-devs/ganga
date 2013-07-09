################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IOutputFile.py,v 0.1 2012-09-28 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *
from fnmatch import fnmatch
from IOutputFile import IOutputFile
from Ganga.Utility.logging import getLogger
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Lib.Job.Job import Job
import re, copy, glob
logger = getLogger()
regex  = re.compile('[*?\[\]]')
import os
import pickle
import httplib2
from apiclient.discovery import build
from apiclient import errors

class GoogleFile(IOutputFile):

    _schema = Schema(Version(1,1),
                     {'namePattern'   : SimpleItem( defvalue="", doc='pattern of the file name'),
                      'localDir'      : SimpleItem( defvalue="",copyable=1,
                                                    doc='local dir where the file is stored, used from get and put methods'),
                      'subfiles'      : ComponentItem( category='outputfiles',defvalue=[], hidden=1,
                                                       typelist=['Ganga.GPIDev.Lib.File.LCGSEFile'], sequence=1, copyable=0,
                                                       doc="collected files from the wildcard namePattern"),
                      'failureReason' : SimpleItem( defvalue="",copyable=1,
                                                    doc='reason for the upload failure'),
                      'compressed'    : SimpleItem( defvalue=False, typelist=['bool'],protected=0,
                                                    doc='wheather the output file should be compressed before sending somewhere'),
                      'downloadURL'   : SimpleItem( defvalue="",copyable=1, hidden=1, protected=1,
                                                    doc='download URL assigned to the file upon upload to GoogleDrive'),
                      'id'            : SimpleItem( defvalue="",copyable=1, hidden=1, protected=1,
                                                    doc='GoogleFile ID assigned on upload to GoogleDrive'),
                      'title'         : SimpleItem( defvalue="",copyable=1, hidden=1, protected=1,
                                                    doc='GoogleFile title of the uploaded file'),
                      })
    _category = 'outputfiles'
    _name = 'GoogleFile'
    _exportmethods = [ "get" , "put", "remove", "restore"]

    def __init__(self, namePattern=''):
        super(GoogleFile, self).__init__()
        self.namePattern = namePattern
        if os.path.isfile("/home/hep/hs4011/GDrive/creddata.pkl") == False :
            from oauth2client.client import OAuth2WebServerFlow

            # Copy your credentials from the APIs Console
            CLIENT_ID = "585533701608.apps.googleusercontent.com"
            CLIENT_SECRET = "-4qvKAG1iqeSGaBEQzHBqxyC"

            # Check https://developers.google.com/drive/scopes for all available scopes
            OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive.file'

            # Redirect URI for installed apps
            REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

            # Run through the OAuth flow and retrieve credentials
            flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
            authorize_url = flow.step1_get_authorize_url()
            print 'Go to the following link in your browser: ' + authorize_url
            code = raw_input('Enter verification code: ').strip()
            credentials = flow.step2_exchange(code)
            
            #Pickle credential data
            output = open("/home/hep/hs4011/GDrive/creddata.pkl","wb")
            pickle.dump(credentials, output)
            output.close()

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(GoogleFile, self).__construct__(args)
        else:
            self.namePattern = args[0]

    def setLocation(self):
        """
        Sets the location of output files that were uploaded from the WN
        """
        raise NotImplementedError

    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        raise NotImplementedError

    def _on_attribute__set__(self, obj_type, attrib_name):
        r = copy.deepcopy(self)
        if isinstance(obj_type, Job) and attrib_name == 'outputfiles':
            r.localDir=None
            r.failureReason=''
        return r

    def get(self):
        """
        Retrieves locally all files that were uploaded before that 
        """
        #Retrieves creddata and sets up service connection
        http = httplib2.Http()
        nput = open("/home/hep/hs4011/GDrive/creddata.pkl","rb")
        credentials = pickle.load(nput)
        nput.close()
        http = credentials.authorize(http)
        service = build('drive', 'v2', http=http)

        #Checks for wildcards and loops through get procedure for each result, saving file to assigned directory
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                if f.downloadURL:
                    resp, content = service._http.request(f.downloadURL)
                    if resp.status == 200:
                        #print 'Status: %s' % resp
                        logger.info("File \'%s\' downloaded succesfully" % f.title)
                        dir_path = f.localDir
                        if f.localDir == '':
                            dir_path = self.localDir
                            if self.localDir =='':
                                dir_path = os.getcwd()
                        completeName = os.path.join(dir_path, f.title)
                        gotfile = open(completeName,"wb")
                        gotfile.write(content)
                        gotfile.close()

                    else:
                        #print 'An error occurred: %s' % resp
                        logger.info("Download unsuccessful, file \'%s\' may not exist on GoogleDrive" % f.title)
                else:
                    # The file doesn't have any content stored on Drive.
                    logger.info("No such file on GoogleDrive")
                    return None

        #Standalone get request procedure
        else:
            if self.downloadURL:
                resp, content = service._http.request(self.downloadURL)
                if resp.status == 200:
                    #
                    gotfile1 = open('/home/hep/hs4011/Test/resp.pkl',"wb")
                    pickle.dump(resp, gotfile1)
                    gotfile1.close()
                    gotfile2 = open('/home/hep/hs4011/Test/content.pkl',"wb")
                    pickle.dump(content, gotfile2)
                    gotfile2.close()
                    #
                    #print 'Status: %s' % resp
                    logger.info("Download successful")
                    dir_path = self.localDir
                    if self.localDir == '':
                        dir_path = os.getcwd()
                    completeName = os.path.join(dir_path, self.namePattern)
                    gotfile = open(completeName,"wb")
                    gotfile.write(content)
                    gotfile.close()
                else:
                    #print 'An error occurred: %s' % resp
                    logger.info("Download unsuccessful")
                    return None
            else:
                #The file doesn't have any content stored on Drive.
                logger.info("No such file on GoogleDrive")
                return None

    def getWNScriptDownloadCommand(self, indent):
        """
        Gets the command used to download already uploaded file
        """
        raise NotImplementedError

    def __repr__(self):
        """Get the representation of the file."""

        return "GoogleFile(namePattern='%s', downloadURL='%s')" % (self.namePattern, self.downloadURL)

    def put(self):
        """
        Postprocesses (upload) output file to the desired destination from the client
        """
        import pprint
        import hashlib
        from apiclient.http import MediaFileUpload

        http = httplib2.Http()
        nput = open("/home/hep/hs4011/GDrive/creddata.pkl","rb")
        credentials = pickle.load(nput)
        nput.close()
        http = credentials.authorize(http)
        service = build('drive', 'v2', http=http)

        #Sets the target directory
        dir_path = self.localDir
        if self.localDir == '':
            dir_path = os.getcwd()

        if self._parent is not None:
            dir_path = self.getJobObject().getOutputWorkspace().getPath()

        #Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for wildfile in glob.glob(os.path.join(dir_path, self.namePattern)):
                FILENAME = wildfile
                filename = os.path.basename(wildfile)

                #Upload procedure
                media_body = MediaFileUpload(FILENAME, mimetype='text/plain', resumable=True)
                body = {
                    'title': '%s' % filename,
                    'description': 'A test document',
                    'mimeType': 'text/plain'
                    }

                #Metadata file and md5checksum intergrity check
                file = service.files().insert(body=body, media_body=media_body).execute()
                with open(FILENAME, 'rb') as thefile:
                    if file.get('md5Checksum')==hashlib.md5(thefile.read()).hexdigest():
                        logger.info("File %s uploaded successfully" % filename)
                    else:
                        logger.error("File %s uploaded unsuccessfully" % filename)

                #Assign new schema components to each file and append to job subfiles
                g = GoogleFile(filename)
                g.downloadURL = file.get('downloadUrl', '')
                g.id          = file.get('id'         , '')
                g.title       = file.get('title'      , '')
                self.subfiles.append(GPIProxyObjectFactory(g))

        #For standalone upload
        else:
            #Path to the file to upload
            FILENAME = os.path.join( dir_path, self.namePattern)

            #Upload procedure, can edit more of file metadata
            media_body = MediaFileUpload(FILENAME, mimetype='text/plain', resumable=True)
            body = {
                'title': '%s'%self.namePattern,
                'description': 'A test document',
                'mimeType': 'text/plain'
                }

            #Metadata storage and md5checksum integrity check
            file = service.files().insert(body=body, media_body=media_body).execute()
            #pprint.pprint(file) #Prints metadata
            with open(FILENAME, 'rb') as thefile:
                if file.get('md5Checksum')==hashlib.md5(thefile.read()).hexdigest():
                    logger.info("Upload Successful")
                    logger.info("File uploaded: %s" % self.namePattern)
                else:
                    logger.error("Upload Unsuccessfull")
                    
            #
            outputf=open('/home/hep/hs4011/Test/file.pkl','wb')
            pickle.dump(file, outputf)
            outputf.close()
            #

            #Assign values to new schema components
            self.downloadURL = file.get('downloadUrl', '')
            self.id          = file.get('id'         , '')
            self.title       = file.get('title'      , '')

            return
        return GPIProxyObjectFactory(self.subfiles[:])

    def remove(self, permanent=False):
        """
        Move a file to the trash or permanently delete the file.
        """
        http = httplib2.Http()
        nput = open("/home/hep/hs4011/GDrive/creddata.pkl","rb")
        credentials = pickle.load(nput)
        nput.close()
        http = credentials.authorize(http)
        service = build('drive', 'v2', http=http)

        #Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                if permanent==True:
                    try:
                        service.files().delete(fileId=f.id).execute()
                        logger.info('File \'%s\' permanently deleted from GoogleDrive' % f.title)
                    except errors.HttpError, error:
                        #print 'An error occurred: %s' % error
                        logger.info('File \'%s\' deletion failed, or file already deleted'% f.title)
                else:
                    try:
                        service.files().trash(fileId=f.id).execute()
                        logger.info('File \'%s\' removed from GoogleDrive' % f.title)
                    except errors.HttpError, error:
                        #print 'An error occurred: %s' % error
                        logger.info('File \'%s\' removal failed, or file already removed'% f.title)

        #Standalone request
        else:
            if permanent==True:
                try:
                    service.files().delete(fileId=self.id).execute()
                    logger.info('File permanently deleted from GoogleDrive')
                except errors.HttpError, error:
                    #print 'An error occurred: %s' % error
                    logger.info('File deletion failed')
            else:
                try:
                    service.files().trash(fileId=self.id).execute()
                    logger.info('File removed from GoogleDrive')
                except errors.HttpError, error:
                    #print 'An error occurred: %s' % error
                    logger.info('File removal failed')
                return None

    def restore(self):
        """Restore a file from the trash.
        """
        http = httplib2.Http()
        nput = open("/home/hep/hs4011/GDrive/creddata.pkl","rb")
        credentials = pickle.load(nput)
        nput.close()
        http = credentials.authorize(http)
        service = build('drive', 'v2', http=http)

        #Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                try:
                    service.files().untrash(fileId=f.id).execute()
                    logger.info('File \'%s\' restored to GoogleDrive' % f.title)
                except errors.HttpError, error:
                    #print 'An error occurred: %s' % error
                    logger.info('File \'%s\' restore failed, or file does not exist on GoogleDrive'% f.title)

        #Standalone request
        else:
            try:
                service.files().untrash(fileId=self.id).execute()
                logger.info('File restored to GoogleDrive')
            except errors.HttpError, error:
                #print 'An error occurred: %s' % error
                logger.info('File restore failed')
            return None

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        logger.info('injecting')

    def _readonly(self):
        return False

    def _list_get__match__(self, to_match):
        if type(to_match) == str:
            return fnmatch(self.namePattern, to_match)
        if type(to_match) == type:
            #note stripProxy wont work on class types that aren't instances
            return isinstance(self, to_match._impl)
        return to_match==self

import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['GoogleFile'] = GoogleFile

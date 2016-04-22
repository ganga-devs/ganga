from __future__ import absolute_import
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from fnmatch import fnmatch
from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
from Ganga.Utility.logging import getLogger
from Ganga.GPIDev.Base.Proxy import isType, GPIProxyObjectFactory
from Ganga.Utility.Config import getConfig
import re
import copy
import glob
import os
import pickle
import stat
import logging
import Ganga.Utility.Config

logger = getLogger()
regex = re.compile('[*?\[\]]')
badlogger = getLogger('oauth2client.util')
badlogger.setLevel(logging.ERROR)

class GoogleFile(IGangaFile):

    """
    The GoogleFile outputfile type allows for files to be directly uploaded, downloaded, removed and restored from the GoogleDrive service.
    It can be used as part of a job to output data directly to GoogleDrive, or standalone through the Ganga interface.

    example job: j=Job(application=Executable(exe=File('/home/hep/hs4011/Tests/testjob.sh'), args=[]),outputfiles=[GoogleFile('TestJob.txt')])

                 j.submit()

                 ### This job will automatically upload the outputfile 'TestJob.txt' to GoogleDrive.

    example of standalone submission:

                 g=GoogleFile('TestFile.txt')

                 g.localDir = '~/TestDirectory'        ### The file's location must be specified for standalone submission

                 g.put()                               ### The put() method uploads the file to GoogleDrive directly

    The GoogleFile outputfile is also compatible with the Dirac backend, making outputfiles from Dirac-run jobs upload directly to GoogleDrive.
    """

    _schema = Schema(Version(1, 1),
                     {'namePattern': SimpleItem(defvalue="", doc='pattern of the file name'),
                      'localDir': SimpleItem(defvalue="", copyable=1,
                                             doc='local dir where the file is stored, used from get and put methods'),
                      'subfiles': ComponentItem(category='gangafiles', defvalue=[], hidden=1,
                                                sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),
                      'failureReason': SimpleItem(defvalue="", copyable=1,
                                                  doc='reason for the upload failure'),
                      'compressed': SimpleItem(defvalue=False, typelist=[bool], protected=0,
                                               doc='wheather the output file should be compressed before sending somewhere'),
                      'downloadURL': SimpleItem(defvalue="", copyable=1, protected=1,
                                                doc='download URL assigned to the file upon upload to GoogleDrive'),
                      'id': SimpleItem(defvalue="", copyable=1, hidden=1, protected=1,
                                       doc='GoogleFile ID assigned to file  on upload to GoogleDrive'),
                      'title': SimpleItem(defvalue="", copyable=1, hidden=1, protected=1,
                                                   doc='GoogleFile title of the uploaded file'),
                      'GangaFolderId': SimpleItem(defvalue="", copyable=1, hidden=1, protected=1,
                                                  doc='GoogleDrive Ganga folder  ID')
                      })
    _category = 'gangafiles'
    _name = 'GoogleFile'
    _exportmethods = ["get", "put", "remove", "restore", "deleteCredentials"]

    def __init__(self, namePattern=''):
        super(GoogleFile, self).__init__()
        self.namePattern = namePattern
        self.__initialized = False
        
        self.cred_path = os.path.join(getConfig('Configuration')['gangadir'], 'googlecreddata.pkl')

    def __initializeCred(self):
        while os.path.isfile(self.cred_path) == False:
            from oauth2client.client import OAuth2WebServerFlow

            # Copy your credentials from the APIs Console
#            CLIENT_ID = "54459939297.apps.googleusercontent.com"
#            CLIENT_SECRET = "mAToHx5RpXtwkeYR6nOIe_Yw"
            CLIENT_ID = '776655306197-dirtoquqsm7cpqgepvamofg5t2b5f637.apps.googleusercontent.com'
            CLIENT_SECRET = 'GpdEP-OBZZQLB3k-xxOpzFQG'
            # Check https://developers.google.com/drive/scopes for all
            # available scopes
            OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive.file'

            # Redirect URI for installed apps
            REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

            # Run through the OAuth flow and retrieve credentials
            credentials = ''
            flow = OAuth2WebServerFlow(
                CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
            authorize_url = flow.step1_get_authorize_url()
            try:
                import webbrowser
                webbrowser.get('macosx').open(authorize_url, 0, True)
            except:
                try:
                    import webbrowser
                    webbrowser.get(
                        'windows-default').open(authorize_url, 0, True)
                except:
                    try:
                        import webbrowser
                        webbrowser.get('firefox').open(authorize_url, 0, True)
                    except Exception, err:
                        logger.error("Error: %s" % str(err))
                        pass
            logger.info(
                'Go to the following link in your browser: ' + authorize_url)
            code = raw_input('Enter verification code: ').strip()
            try:
                credentials = flow.step2_exchange(code)
            except:
                deny = raw_input(
                    'An incorrect code was entered. Have you denied Ganga access to your GoogleDrive (y/[n])?')
                if deny == '' or deny[0:1].upper() == 'N':
                    pass
                elif deny[0:1].upper() == 'Y':
                    return None

            # Pickle credential data
            if credentials is not '':
                with open(self.cred_path, "wb") as output:
                    pickle.dump(credentials, output)
                
                os.chmod(self.cred_path, stat.S_IWUSR | stat.S_IRUSR)
                logger.info('Your GoogleDrive credentials have been stored in the file %s and are only readable by you. '
                            'The file will give permission to modify files in your GoogleDrive. '
                            'Permission can be revoked by going to "Manage Apps" in your GoogleDrive '
                            'or by deleting the credentials through the deleteCredentials GoogleFile method.' % self.cred_path)

        self.__initialized = True

        self._check_Ganga_folder()

    def __construct__(self, args):
        if (len(args) != 1) or (not isType(args[0], str)):
            super(GoogleFile, self).__construct__(args)
        else:
            self.namePattern = args[0]
        self.cred_path = os.path.join(getConfig('Configuration')['gangadir'], 'googlecreddata.pkl')

    def _attribute_filter__set__(self, n, v):
        if n == 'localDir':
            return os.path.expanduser(os.path.expandvars(v))
        return v

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
        from Ganga.GPIDev.Lib.Job import Job
        if isinstance(obj_type, Job) and attrib_name == 'outputfiles':
            r.localDir = None
            r.failureReason = ''
        return r

    def deleteCredentials(self):
        """
        Deletes the user's GoogleDrive credentials

            example use: GoogleFile().deleteCredentials()
        """
        if self.__initilized == True:
            if os.path.isfile(self.cred_path) == True:
                os.remove(self.cred_path)
                logger.info('GoogleDrive credentials deleted')
                return None
        else:
            logger.info('There are no credentials to delete')

    def get(self):
        """
        Retrieves files uploaded to GoogleDrive through a job or by a standalone GoogleFile
        """
        service = self._setup_service()

        # Checks for wildcards and loops through get procedure for each result,
        # saving file to assigned directory
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                if f.downloadURL:
                    resp, content = service._http.request(f.downloadURL)
                    if resp.status == 200:
                        # print 'Status: %s' % resp
                        logger.info(
                            "File \'%s\' downloaded succesfully" % f.title)
                        dir_path = f.localDir
                        if f.localDir == '':
                            dir_path = self.localDir
                            if self.localDir == '':
                                dir_path = os.getcwd()
                        completeName = os.path.join(dir_path, f.title)
                        with open(completeName, "wb") as gotfile:
                            gotfile.write(content)

                    else:
                        # print 'An error occurred: %s' % resp
                        logger.info(
                            "Download unsuccessful, file \'%s\' may not exist on GoogleDrive" % f.title)
                else:
                    # The file doesn't have any content stored on Drive.
                    logger.info(
                        "No file \'%s\' exists on GoogleDrive" % f.title)
                    return None

        # Non-wildcard get request procedure
        else:
            if self.downloadURL:
                resp, content = service._http.request(self.downloadURL)
                if resp.status == 200:
                    # print 'Status: %s' % resp
                    logger.info("Download successful")
                    dir_path = self.localDir
                    if self.localDir == ('' or None):
                        dir_path = os.getcwd()
                    if self._getParent() is not None:
                        dir_path = self.getJobObject(
                        ).getOutputWorkspace().getPath()
                    completeName = os.path.join(dir_path, self.namePattern)
                    with open(completeName, "wb") as gotfile:
                        gotfile.write(content)
                else:
                    # print 'An error occurred: %s' % resp
                    logger.info(
                        "Download unsuccessful, the file may not exist on GoogleDrive")
                    return None
            else:
                # The file doesn't have any content stored on Drive.
                logger.info("No such file on GoogleDrive")
                return

    def getWNScriptDownloadCommand(self, indent):
        """
        Gets the command used to download already uploaded file
        """
        raise NotImplementedError

    def processWildcardMatches(self):
        raise NotImplementedError

    def __repr__(self):
        """
        Get the representation of the file
        """
        return "GoogleFile(namePattern='%s', downloadURL='%s')" % (self.namePattern, self.downloadURL)

    def put(self):
        """
        Postprocesses (upload) output file to the desired destination from the client
        """
        import hashlib
        from apiclient.http import MediaFileUpload

        service = self._setup_service()

        # Sets the target directory
        dir_path = self.localDir
        if self.localDir == '':
            dir_path = os.getcwd()

        if self._getParent() is not None:
            dir_path = self.getJobObject().getOutputWorkspace().getPath()

        # Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for wildfile in glob.glob(os.path.join(dir_path, self.namePattern)):
                FILENAME = wildfile
                filename = os.path.basename(wildfile)

                # Upload procedure
                media_body = MediaFileUpload(
                    FILENAME, mimetype='text/plain', resumable=True)
                body = {
                    'title': '%s' % filename,
                    'description': 'A test document',
                    'mimeType': 'text/plain',
                    'parents': [{
                        "kind": "drive#fileLink",
                        "id": "%s" % self.GangaFolderId
                    }]
                }

                # Metadata file and md5checksum intergrity check
                file = service.files().insert(
                    body=body, media_body=media_body).execute()
                with open(FILENAME, 'rb') as thefile:
                    if file.get('md5Checksum') == hashlib.md5(thefile.read()).hexdigest():
                        logger.info("File \'%s\' uploaded successfully" % filename)
                    else:
                        logger.error(
                            "File \'%s\' uploaded unsuccessfully" % filename)

                # Assign new schema components to each file and append to job
                # subfiles
                g = GoogleFile(filename)
                g.downloadURL = file.get('downloadUrl', '')
                g.id = file.get('id', '')
                g.title = file.get('title', '')
                self.subfiles.append(GPIProxyObjectFactory(g))

        # For non-wildcard upload
        else:
            # Path to the file to upload
            FILENAME = os.path.join(dir_path, self.namePattern)

            # Upload procedure, can edit more of file metadata
            media_body = MediaFileUpload(
                FILENAME, mimetype='text/plain', resumable=True)
            body = {
                'title': '%s' % self.namePattern,
                'description': 'A test document',
                'mimeType': 'text/plain',
                'parents': [{
                    "kind": "drive#fileLink",
                    "id": "%s" % self.GangaFolderId
                }]
            }

            # Metadata storage and md5checksum integrity check
            file = service.files().insert(
                body=body, media_body=media_body).execute()

            with open(FILENAME, 'rb') as thefile:
                if file.get('md5Checksum') == hashlib.md5(thefile.read()).hexdigest():
                    logger.info("File \'%s\' uploaded succesfully" %
                                self.namePattern)
                else:
                    logger.error("Upload Unsuccessful")

            # Assign values to new schema components
            self.downloadURL = file.get('downloadUrl', '')
            self.id = file.get('id', '')
            self.title = file.get('title', '')

            return
        return GPIProxyObjectFactory(self.subfiles[:])

    def remove(self, permanent=False):
        """
        Move a file to the trash or permanently delete the file

            example use: GoogleFile().remove()

            or:          j = Job([...], outputfiles=GoogleFile()) --> j.submit --> j.outputfiles[0].remove()

        Remove multiple files by using

                         for i in j.outputfiles:
                             i.remove()

        The file can also be permanently deleted by using

                         GoogleFile().remove(True)

        However, this will make the file unrestorable
        """
        service = self._setup_service()

        from apiclient import errors

        # Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                if permanent == True:
                    try:
                        service.files().delete(fileId=f.id).execute()
                        f.downloadURL = ''
                        logger.info(
                            'File \'%s\' permanently deleted from GoogleDrive' % f.title)
                    except errors.HttpError as error:
                        # print 'An error occurred: %s' % error
                        logger.info(
                            'File \'%s\' deletion failed, or file already deleted' % f.title)
                else:
                    try:
                        service.files().trash(fileId=f.id).execute()
                        logger.info(
                            'File \'%s\' removed from GoogleDrive' % f.title)
                    except errors.HttpError as error:
                        # print 'An error occurred: %s' % error
                        logger.info(
                            'File \'%s\' removal failed, or file already removed' % f.title)

        # Non-wildcard request
        else:
            if permanent == True:
                try:
                    service.files().delete(fileId=self.id).execute()
                    self.downloadURL = ''
                    logger.info('File permanently deleted from GoogleDrive')
                except errors.HttpError as error:
                    # print 'An error occurred: %s' % error
                    logger.info(
                        'File deletion failed, or file already deleted')
            else:
                try:
                    service.files().trash(fileId=self.id).execute()
                    logger.info('File removed from GoogleDrive')
                except errors.HttpError as error:
                    # print 'An error occurred: %s' % error
                    logger.info('File removal failed, or file already removed')
                return None

    def restore(self):
        """
        Restore a file from the trash. This method will not work on permanently deleted files

            example use: GoogleFile().restore()
        """
        from apiclient import errors

        service = self._setup_service()

        # Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                try:
                    service.files().untrash(fileId=f.id).execute()
                    logger.info(
                        'File \'%s\' restored to GoogleDrive' % f.title)
                except errors.HttpError as error:
                    # print 'An error occurred: %s' % error
                    logger.info(
                        'File \'%s\' restore failed, or file does not exist on GoogleDrive' % f.title)

        # Non-wildcard request
        else:
            try:
                service.files().untrash(fileId=self.id).execute()
                logger.info('File restored to GoogleDrive')
            except errors.HttpError as error:
                # print 'An error occurred: %s' % error
                logger.info(
                    'File restore failed, or file does not exist on GoogleDrive')
            return None

    def _check_Ganga_folder(self):
        """
        Creates a Ganga folder on GoogleDrive if one is not already present
        """
        from apiclient import errors
        service = self._setup_service()

        page_token = None
        try:
            param = {}
            if page_token:
                param['pageToken'] = page_token
            files = service.files().list(**param).execute()
            items = files['items']
            for i in items:
                if i['title'] == 'Ganga':
                    self.GangaFolderId = i['id']
                    return
            page_token = files.get('nextPageToken')
        except errors.HttpError as error:
            logger.info('Failed to create Ganga folder on GoogleDrive')
            # print 'An error occurred: %s' % error

        body = {
            'title': 'Ganga',
            'description': 'A test folder',
            'mimeType': 'application/vnd.google-apps.folder'
        }
        file = service.files().insert(body=body).execute()
        self.GangaFolderId = file.get('id')

    def _setup_service(self):
        """
        Sets up the GoogleDrive service for other methods
        """
        from apiclient.discovery import build
        import httplib2
        http = httplib2.Http()
        if self.__initialized == False:
            self.__initializeCred()
        with open(self.cred_path, "rb") as nput:
            credentials = pickle.load(nput)
        http = credentials.authorize(http)
        service = build('drive', 'v2', http=http)
        return service

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        logger.info('injecting')

    def _readonly(self):
        return False

    def _list_get__match__(self, to_match):
        if isinstance(to_match, str):
            return fnmatch(self.namePattern, to_match)
        if isinstance(to_match, type):
            # note stripProxy wont work on class types that aren't instances
            return isinstance(self, to_match._impl)
        return to_match == self

Ganga.Utility.Config.config_scope['GoogleFile'] = GoogleFile

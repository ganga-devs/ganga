
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from fnmatch import fnmatch
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
from GangaCore.Core.exceptions import GangaFileError
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import isType, GPIProxyObjectFactory
from GangaCore.Utility.Config import getConfig
import re
import copy
import glob
import os
import pickle
import stat
import logging
import GangaCore.Utility.Config

logger = getLogger()
regex = re.compile(r'[*?\[\]]')


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
                      'name': SimpleItem(defvalue="", copyable=1, hidden=1, protected=1,
                                                   doc='GoogleFile name of the uploaded file'),
                      'GangaFolderId': SimpleItem(defvalue="", copyable=1, hidden=1, protected=1,
                                                  doc='GoogleDrive Ganga folder  ID')
                      })
    _category = 'gangafiles'
    _name = 'GoogleFile'
    _exportmethods = ["get", "put", "remove", "restore", "deleteCredentials", "accessURL"]

    def __init__(self, namePattern=''):
        super(GoogleFile, self).__init__()
        self.namePattern = namePattern
        self.__initialized = False

        self.cred_path = os.path.join(getConfig('Configuration')[
                                      'gangadir'], 'googlecreddata.pkl')

    def __initializeCred(self):
        while os.path.isfile(self.cred_path) == False:
            from google.auth.transport.requests import Request
            from google_auth_oauthlib.flow import InstalledAppFlow

            SCOPES = ['https://www.googleapis.com/auth/drive.file']

            creds = None

            account_details = {
                "installed": {
                    "project_id": "ganga-file-uploader",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_secret": "-hvpFfe29n5jhUxAXZFlqxxw",
                    "client_id": "893863581947-l7cqdtsa6q9dn8d3cb1lplqsj9odgqia.apps.googleusercontent.com",
                    "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
                }
            }

            # If there are no (valid) credentials available, let the user log in.
            if not os.path.exists(self.cred_path):
                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                        logger.info(
                            'Enter you accound details in the browser window prompted')
                    else:
                            flow = InstalledAppFlow.from_client_config(
                                account_details, 
                                SCOPES
                            )                        
                            creds = flow.run_local_server(port=0)

                    # Save the credentials for the next run
                    with open(self.cred_path, 'wb') as token:
                        pickle.dump(creds, token)

                    os.chmod(self.cred_path, stat.S_IWUSR | stat.S_IRUSR)
                    logger.info('Your GoogleDrive credentials have been stored in the file %s and are only readable by you. '
                                'The file will give permission to modify files in your GoogleDrive. '
                                'Permission can be revoked by going to "Manage Apps" in your GoogleDrive '
                                'or by deleting the credentials through the deleteCredentials GoogleFile method.' % self.cred_path)

        self.__initialized = True
        self._check_Ganga_folder()

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
        from GangaCore.GPIDev.Lib.Job import Job
        if isinstance(obj_type, Job) and attrib_name == 'outputfiles':
            r.localDir = None
            r.failureReason = ''
        return r

    def deleteCredentials(self):
        """
        Deletes the user's GoogleDrive credentials

            example use: GoogleFile().deleteCredentials()
        """
        if os.path.isfile(self.cred_path) == True:
            os.remove(self.cred_path)
            logger.info('GoogleDrive credentials deleted')
            return None
        else:
            logger.info('There are no credentials to delete')

    def internalCopyTo(self, targetPath):
        """
        Retrieves files uploaded to GoogleDrive
        Args:
            targetPath (str): Target path where the file is copied to
        """
        import io
        from googleapiclient.http import MediaIoBaseDownload

        dir_path = targetPath
        service = self._setup_service()

        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)

        # Checks for wildcards and loops through get procedure for each result,
        # saving file to assigned directory
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                if f.id:
                    completeName = os.path.join(dir_path, f.name)
                    self.download_file_from_drive(service, f.id, completeName)

                else:
                    logger.info("Download unsuccessful, file \'%s\' may not exist on GoogleDrive" % f.name)

        # Non-wildcard get request procedure
        else:
            if self.id:
                completeName = os.path.join(dir_path, self.name)
                self.download_file_from_drive(service, self.id, completeName)

            else:
                # print 'An error occurred: %s' % resp
                logger.info(
                    "Download unsuccessful, the file may not exist on GoogleDrive")
                return None

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

    def accessURL(self):
        if self.subfiles:
            URLs = []
            for f in self.subfiles:
                URLs.append(f.downloadURL)
        elif self.id:
            URLs = self.downloadURL
        return URLs

    def download_file_from_drive(self, service, fileid, filepath, filename=None):
        import io
        from googleapiclient.http import MediaIoBaseDownload

        # if file name is not known, we first get the file's name
        if filename == None:
            name_request = service.files().get(fileId=fileid).execute()
            fname = name_request['name']
            filename = os.path.join(filepath, fname)

        request = service.files().get_media(fileId=fileid)
        fh = io.FileIO(filename, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logger.info(f"Downloading file: {self.name} {int(status.progress()*100)}")
        logger.info("Download successful")

    def get(self):
        """
        Method to get the Local file from files uploaded to GoogleDrive by ganga
        """
        service = self._setup_service()

        # Sets the target directory
        dir_path = self.localDir
        if self.localDir == '':
            dir_path = os.getcwd()

        if self._getParent() is not None:
            dir_path = self.getJobObject().getOutputWorkspace().getPath()

        # Wildcard procedure
        if regex.search(self.namePattern) is not None:
            import fnmatch

            files_found = False
            file_regex = fnmatch.translate(self.namePattern)

            search_result = service.files().list(
                q=f"'{self.GangaFolderId}' in parents",
                spaces='drive'
            ).execute() 

            if search_result['files']:
                for _file in search_result['files']:
                    if re.match(file_regex, _file['name']) is not None:
                        files_found = True
                        self.download_file_from_drive(
                            service, _file['id'], dir_path, None
                        )
                if files_found is False:
                    raise GangaFileError(f"No files with pattern: {self.namePattern} were found in Ganga Folder of your Google Drive. Ganga can only see files that was uploded by Ganga itself.")
            else:
                raise GangaFileError(f"Ganga Folder of your Google Drive is empty/non-existent")
        else:
            search_result = service.files().list(
                q=f"name = '{self.namePattern}' and parents in '{self.GangaFolderId}'",
                spaces='drive'
            ).execute() 

            if search_result['files']:
                for _file in search_result['files']:
                    self.download_file_from_drive(
                        service, _file['id'], dir_path, self.namePattern
                    )
            else:
                raise GangaFileError(f"File: {self.namePattern} not found in Ganga Folder of your Google Drive. Ganga can only see files that was uploded by Ganga itself.")

    def put(self):
        """
        Postprocesses (upload) output file to the desired destination from the client
        """
        import hashlib
        from googleapiclient.http import MediaFileUpload

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
                # checking if the to be uploaded exists
                if not os.path.isfile(FILENAME):
                    raise GangaFileError(f"File: {FILENAME} not found.")

                file_metadata = {
                    'name': filename,
                    'description': 'A test document',
                    'mimeType': 'text/plain',
                    'parents': [self.GangaFolderId]
                }
                media = MediaFileUpload(
                    FILENAME,
                    mimetype='application/vnd.google-apps.document'
                )
                file = service.files().create(
                    fields='id',
                    media_body=media,
                    body=file_metadata
                ).execute()

                # Checking the hash of inserted data
                with open(FILENAME, 'rb') as thefile:
                    file_results = service.files().list(
                        q=f"name='{filename}'",
                        fields="nextPageToken, files(id, name, md5Checksum)"
                    ).execute()

                    for _file in file_results.get('files', []):
                        # Found the file's information that was just uploaded
                        if _file['id'] == file['id']:
                            if _file['md5Checksum'] == hashlib.md5(thefile.read()).hexdigest():
                                logger.info("File \'%s\' uploaded succesfully" %
                                            filename)
                            else:
                                raise GangaFileError("Upload of \'%s\' unsuccessful" % filename)

                # Assign new schema components to each file and append to job
                # subfiles
                g = GoogleFile(filename)
                g.downloadURL = f"https://drive.google.com/file/d/{file['id']}"
                g.id = file['id']
                g.name = file_metadata['name']
                self.subfiles.append(GPIProxyObjectFactory(g))

        # For non-wildcard upload
        else:
            FILENAME = os.path.join(dir_path, self.namePattern)
            if not os.path.isfile(FILENAME):
                raise GangaFileError(f"File: {FILENAME} not found.")

            file_metadata = {
                'name': self.namePattern,
                'description': 'A test document',
                'mimeType': 'text/plain',
                'parents': [self.GangaFolderId]
            }
            media = MediaFileUpload(
                FILENAME,
                mimetype='application/vnd.google-apps.document'
            )
            file = service.files().create(
                fields='id',
                media_body=media,
                body=file_metadata
            ).execute()

            # Checking the hash of inserted data
            with open(FILENAME, 'rb') as thefile:
                file_results = service.files().list(
                    q=f"name='{self.namePattern}'",
                    fields="nextPageToken, files(id, name, md5Checksum)"
                ).execute()

                for _file in file_results.get('files', []):
                    # Found the file's information that was just uploaded
                    if _file['id'] == file['id']:
                        if _file['md5Checksum'] == hashlib.md5(thefile.read()).hexdigest():
                            logger.info("File \'%s\' uploaded succesfully" %
                                        self.namePattern)
                        else:
                            raise GangaFileError("Upload of \'%s\' unsuccessful" % self.namePattern)

            # Assign values to new schema components
            self.downloadURL = f"https://drive.google.com/file/d/{file['id']}"
            self.id = file['id']
            self.name = file_metadata['name']
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
        from googleapiclient.errors import HttpError 
        service = self._setup_service()

        # Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                if permanent == True:
                    try:
                        service.files().delete(fileId=f.id).execute()
                        f.downloadURL = ''
                        logger.info(
                            'File \'%s\' permanently deleted from GoogleDrive' % f.name)
                    except HttpError as error:
                        # print 'An error occurred: %s' % error
                        logger.info(
                            'File \'%s\' deletion failed, or file already deleted' % f.name)
                else:
                    try:
                        # updating the file metadata to trash it
                        service.files().update(
                            fileId=f.id,
                            body={"trashed": True}
                        ).execute()
                        logger.info(
                            'File \'%s\' removed from GoogleDrive, added to trash' % f.name)
                    except HttpError as error:
                        # print 'An error occurred: %s' % error
                        logger.info(
                            'File \'%s\' removal failed, or file already removed' % f.name)

        # Non-wildcard request
        else:
            if permanent == True:
                try:
                    service.files().delete(fileId=self.id).execute()
                    self.downloadURL = ''
                    logger.info('File permanently deleted from GoogleDrive')
                except HttpError as error:
                    logger.info(
                        'File deletion failed, or file already deleted')
            else:
                try:
                    # updating the file metadata to trash it
                    service.files().update(
                        fileId=self.id,
                        body={"trashed": True}
                    ).execute()
                    logger.info('File removed from GoogleDrive, added to trash')
                except HttpError as error:
                    logger.info('File removal failed, or file already removed')
                return None

    def restore(self):
        """
        Restore a file from the trash. This method will not work on permanently deleted files

            example use: GoogleFile().restore()
        """
        from googleapiclient.errors import HttpError 

        service = self._setup_service()

        # Wildcard procedure
        if regex.search(self.namePattern) is not None:
            for f in self.subfiles:
                try:
                    service.files().update(
                        fileId=f.id,
                        body={"trashed": False}
                    ).execute()                 
                    logger.info(
                        'File \'%s\' restored to GoogleDrive' % f.name)
                except HttpError as error:
                    # print 'An error occurred: %s' % error
                    logger.info(
                        'File \'%s\' restore failed, or file does not exist on GoogleDrive' % f.name)
                return None
        # Non-wildcard request
        else:
            try:
                service.files().update(
                    fileId=self.id,
                    body={"trashed": False}
                ).execute()
                logger.info(
                        'File \'%s\' restored to GoogleDrive' % self.name)
            except HttpError as error:
                # print 'An error occurred: %s' % error
                logger.info(
                        'File \'%s\' restore failed, or file does not exist on GoogleDrive' % self.name)
            return None

    def _check_Ganga_folder(self):
        """
        Creates a Ganga folder on GoogleDrive if one is not already present
        """
        service = self._setup_service()

        # grabing all the folders in root folder of gdrive 
        results = service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name)"
        ).execute()
        items = results.get('files', [])

        for _file in items:
            if _file['name'] == 'Ganga':
                self.GangaFolderId = _file['id']
                
        if not self.GangaFolderId:
            body = {
                'name': 'Ganga',
                'description': 'A test folder',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            file = service.files().create(body=body).execute()
            self.GangaFolderId = file.get('id')

    def _setup_service(self):
        """
        Sets up the GoogleDrive service for other methods
        """
        from googleapiclient.discovery import build
        if self.__initialized == False:
            self.__initializeCred()
        with open(self.cred_path, "rb") as nput:
            credentials = pickle.load(nput)
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
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

GangaCore.Utility.Config.config_scope['GoogleFile'] = GoogleFile

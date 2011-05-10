# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.template import RequestContext 
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User

from file_upload import UploadFileForm
from models import Report, MonitoringLink

from monitoringsite import settings
import os
import tarfile
import datetime
import mimetypes
import base64

import simplejson as json
from datetime import datetime

#gets meta information from ganga tarball -> in case the report is submitted from ganga
def get_username_version_jobuuid_from_archive(tarFilePath):

        tarFolder = os.path.basename(tarFilePath)
        index = tarFolder.find('.tar')
        tarFolder = tarFolder[:index]
        #get path in the tar to envoron.txt and userconfig.txt files
        environFileName = os.path.join(tarFolder, 'environ.txt')
        userconfigFileName = os.path.join(tarFolder, 'userconfig.txt') 

        username = ''
        gangaVersion = ''
        jobuuid = ''    

        jobFullPrintFileName = ''

        tar = tarfile.open(tarFilePath)

        #get path to job fullprint file - it is not in the home directory so search in the directory tree
        for fileName in tar.getnames():
                if fileName.find('jobfullprint.txt') > -1:
                        jobFullPrintFileName = fileName
                        break

        #get the file instances
        envFile = tar.extractfile(environFileName)
        configFile = tar.extractfile(userconfigFileName)
        jobFile = None

        if jobFullPrintFileName is not '' :
                jobFile = tar.extractfile(jobFullPrintFileName)

        try:
                #read username
                fileLines = envFile.readlines()

                for line in fileLines:
                        if line.find('USER:') == 0:
                                username = line[5:].strip()

                #read ganga version
                fileLines = configFile.readlines()

                for line in fileLines:
                        if line.find('#GANGA_VERSION') == 0:
                                gangaVersion = line[16:].strip()

                #read job uuid
                if jobFile is not None:
                        fileLines = jobFile.readlines()

                        for line in fileLines:
                                if line.find('uuid =') > -1:
                                        jobuuid = line[line.find('\'')+1:line.rfind('\'')]

                
        finally:
                
                if jobFile is not None:
                        jobFile.close()
        
                envFile.close()
                configFile.close()
                tar.close()

                

        return (username, gangaVersion, jobuuid)

#gets meta info from lines that are read from meta file
def extractMetaInfoFromLines(fileLines):

        #result dictionary that contains string values for username, version, jobuuid and dictionary value for monlinks
        result = {}
        monlinks = {}

        for line in fileLines:

                if line.find('username:') == 0:
                        result['user'] = line[9:].strip()               

                elif line.find('version:') == 0:
                        result['version'] = line[8:].strip()            

                elif line.find('jobuuid:') == 0:
                        result['uuid'] = line[8:].strip()

                #fill monlinks with key the name and value the path to the link
                elif line.find('monitoringlink:') == 0:
                        monlink = line[15:].strip()
                        linkName, linkPath = monlink.split(',')[0], monlink.split(',')[1]
                        monlinks[linkName] = linkPath

        result['monlinks'] = monlinks

        return result   

#gets metadata in case the report is zipped
def getReportMetadataZip(zipFilePath):

        import zipfile
        z = zipfile.ZipFile(zipFilePath, "r")

        result = {}
        
        #find the metadata file in the directory tree
        for filename in z.namelist():
                if os.path.basename(filename) == '__metadata.txt':
                        try:
                                #extract metainfo from file lines
                                return extractMetaInfoFromLines(z.read(filename).split('\n'))
                        finally:
                                z.close()       

#checks if metadatafile exists and if yes gets the info from there, else like it was before - extracting metadata from diff files
def getReportMetadataTar(tarFilePath):

        tarFolder = os.path.basename(tarFilePath)

        tar = tarfile.open(tarFilePath)

        metaDataExists = False
        metaDataFileName = ''

        #search in directory tree for metadata file
        for fileName in tar.getnames():
                if os.path.basename(fileName) == '__metadata.txt':
                        metaDataExists = True
                        metaDataFileName = fileName
                        break
                        
        result = {}

        #if it exists set the result dictionary from it
        if metaDataExists:

                metaFile = tar.extractfile(metaDataFileName)

                try:
                        result = extractMetaInfoFromLines(metaFile.readlines())
                finally:
                        metaFile.close()

        #else extract the metainfo from diferent files in the tarball
        else:
                
                user, version, jobuuid = get_username_version_jobuuid_from_archive(tarFilePath)
                result['user'] = user
                result['version'] = version 
                result['uuid'] = jobuuid

                monlinks = {}

                #in case of tarball with no metafile, set one monitoring link for the job
                if jobuuid is not '':

                        urlPrefix = 'http://gangamon.cern.ch/ganga.html#user=&query='
                        monlinks['job monitoring'] = urlPrefix + base64.encodestring('uuid:' + jobuuid)

                result['monlinks'] = monlinks

        return result

#gets the application folder where views.py is stored
def getApplicationFolder():
        
        #get the community from the settings file
        community = getCommunityAndDebugMode()[0]

        if community == 'CMS':
                return 'cmserrorreport'
        elif community == 'Ganga':
                return 'gangaerrorreport'

        return 'gangaerrorreport'

#reads community and debug mode from the settings file
#debug mode -> whether it is on development environment or uploaded on the server
def getCommunityAndDebugMode():

        settingsFile = open(settings.MEDIA_ROOT+'cmserrorreport/scripts/settings.js', 'r')

        community = 'Ganga'
        debug = True

        #find the line for the desired setting and get its value
        try:
                lines = settingsFile.readlines()

                for line in lines:
                        if line.find('var settings_COMMUNITY =') == 0:
                                community = line[24:].strip().strip('\'')
                        if line.find('var settings_DEBUG =') == 0:
                                debug = (line[20:].strip().strip('\'') == 'True')
                
        finally:
                 settingsFile.close()

        return community, debug
        
#uploads the file from the http POST request
def handle_uploaded_file(f):

        filename = os.path.basename(f.name)
        
        #get the community to know in which folder to upload it 
        community = getCommunityAndDebugMode()[0]
        
        communityFolder = 'Ganga'

        if community == 'CMS':
                communityFolder = 'CMS'

        #construct the upload path
        serverPath = os.path.join(settings.UPLOAD_SERVER_PATH, communityFolder)

        if not os.path.exists(serverPath):
                os.mkdir(serverPath)

        #construct the destination file name
        destinationFilePath = os.path.join(serverPath, filename)
        destination = open(destinationFilePath, 'ab+')

        #write the file by chunks reading from the http POST
        for chunk in f.chunks():
                destination.write(chunk)
        destination.close()

        #returns the path to the uploaded file
        return destinationFilePath

#renames the uploaded archive -> the new name is the report id , file extension is kept 
def rename_file(filePath, id, fileExtension):

        path, fileName = os.path.split(filePath)

        index = -1

        if fileExtension == 'tar':
                index = fileName.find('.tar')
        elif fileExtension == 'zip':
                index = fileName.find('.zip')

        #get the extension
        extension = fileName[index:]

        #construct the new file name and file path
        newTarFileName = str(id) + extension
        resultPath = os.path.join(path, newTarFileName)

        #rename the archive
        os.rename(filePath, resultPath)

        return resultPath

#get content type by filename
def get_content_type (filename):
        return mimetypes.guess_type (filename)[0] or 'application/octet-stream'

#login view for authorization and downloading of the report
def login(request, reportId):

        #get the application folder -> ganga or cms
        applicationFolder = getApplicationFolder()

        #if request method is get we should redirect to the login page for authorization
        if request.method == 'GET':
                return render_to_response('%s/login.html' % applicationFolder, {'reportId': reportId, 'display_error_message': 'none'}, context_instance = RequestContext(request))
        #if request is post
        elif request.method == 'POST':

                #get user and pass from the post request
                user = request.POST['user']
                password = request.POST['pass']
                
                dbUser = None

                #try to load the user with this username -> username is unique
                try:
                        dbUser = User.objects.get(username=user)
                except User.DoesNotExist:
                        pass    

                #display error message is set to js 'visible'
                display_error_message = 'block'

                #if we loaded the user by username, check if the password is correct
                if dbUser is not None:
                        password_correct = dbUser.check_password(password)
                        if password_correct: 
                                #display error message is set to js 'unvisible'
                                display_error_message = 'none'
                                # send the file to the browser
                                return send_file_to_response(request, reportId)
                
                #stay at the same page and show error message that user and pass are not correct
                return render_to_response('%s/login.html' % applicationFolder, {'reportId': reportId, 'display_error_message': display_error_message}, context_instance = RequestContext(request))    
                
#downloads the report
def download(request, reportId):

        #sets the appname folder
        appName = "errorreports"

        community, debug = getCommunityAndDebugMode()[0], getCommunityAndDebugMode()[1]

        if community == 'CMS':
                appName = "cmserrorreports"
        
        #for CMS pass the autorization if the autorization cookie is still there
        if community == 'CMS':
                if request.COOKIES.has_key('authorized'):
                        return send_file_to_response(request, reportId)

        #redirect url for authorization
        redirectUrl = '/%s/login/%s' % (appName, reportId)

        if debug:
                redirectUrl = '/%s/login/%s' % (appName, reportId)

        #make the redirect
        response = HttpResponseRedirect(redirectUrl)

        return response

#sends the file to response
def send_file_to_response(request, reportId):

        communityFolder = 'Ganga'

        community = getCommunityAndDebugMode()[0]

        if community == 'CMS':
                communityFolder = 'CMS'

        #constructs the download folder for the application - ganga or cms
        serverPath = os.path.join(settings.UPLOAD_SERVER_PATH, communityFolder)
        
        reportName = ''

        #list the files in the download folder
        reports = os.listdir(serverPath)

        #find which report name (without the extension) is the same as the report id
        for report in reports:
                dotIndex = report.find('.')
                if dotIndex > -1 and report[:dotIndex] == str(reportId):
                        reportName = report
                        break   

        #construct the full download path
        downloadPath = os.path.join(serverPath, reportName)

        file = open(downloadPath, 'rb')

        try:
                #read  the file bytes
                fileBytes = file.read()
        finally:
                file.close()

        #send the file bytes to the response
        response = HttpResponse(fileBytes, content_type=get_content_type(reportName), mimetype=get_content_type(reportName))
        response['Content-Disposition'] = 'attachment; filename=%s' % reportName

        #for CMS if the cookie has expired and we are downloading a report, set another cookie
        if community == 'CMS':
                if not request.COOKIES.has_key('authorized'):
                        response.set_cookie('authorized', value="completed", max_age=24*60*60)

        return response 

#gets the reports from the database and returns the JSON for them
def get_reports_JSON(request):
        
        import cgi
        import datetime

        table_data = []
        
        #load the reports
        reports = Report.objects.order_by('-id').all()

        #get the query string
        queryString = request.META['QUERY_STRING']
        
        queryStringDict = dict(cgi.parse_qsl(queryString))

        #filter the reports by username(checks for exact match), from and till date
        if queryStringDict.has_key('user'):
                reports = reports.filter(username=queryStringDict['user'])
        
        if queryStringDict.has_key('from'):
                fromDate = datetime.datetime.fromtimestamp(int(queryStringDict['from']))
                reports = reports.filter(date_uploaded__gte=fromDate)

        if queryStringDict.has_key('till'):
                tillDate = datetime.datetime.fromtimestamp(int(queryStringDict['till']))
                reports = reports.filter(date_uploaded__lte=tillDate)

        #for version the filter is made by sql LIKE command
        if queryStringDict.has_key('query'):
                version = queryStringDict['query']
                if version is not '':
                        reports = reports.filter(ganga_version__contains=version)               
                
        #iterate and append the JSON for each report
        for report in reports:
                table_data.append(report2json(report))

        #return the JSON
        return HttpResponse(json.dumps(table_data), mimetype='application/json')

#constructs JSON for a report object
def report2json(report):

        #get the monitoring links for the report
        reportMonitoringLinks = MonitoringLink.objects.filter(report__id=report.id)

        #for all monitoring links create one html that will be added in the JSON
        monitoringLinksHtml = ""

        #for each mon link create html a tag and new line tag and add it to the html 
        for reportMonLink in reportMonitoringLinks:
                monitoringLinksHtml += "<a href=%s>%s</a></br>" % (reportMonLink.monitoring_link_path, reportMonLink.monitoring_link_name) 

        #get the appropriate application name for construction of the download link
        appName = "errorreports"

        community, debug = getCommunityAndDebugMode()[0], getCommunityAndDebugMode()[1]

        if community == 'CMS':
                appName = "cmserrorreports"

        #construct the download link
        downloadLink = "<a href=\"/%s/download/%s\">Download</a>" % (appName, str(report.id))

        #in case of development version it is slightly different - without django
        if debug:
                downloadLink = "<a href=\"/%s/download/%s\">Download</a>" % (appName, str(report.id))

        #return JSON for the report object
        return [
                report.id,
                report.date_uploaded.strftime("%Y-%m-%d %H:%M:%S"),    
                report.username,                        
                report.ganga_version,                      
                monitoringLinksHtml,
                downloadLink
         ]

def logServerResponse(server_response):

        #get the communityFolder to know in which folder to upload it 
        communityFolder = getCommunityAndDebugMode()[0]

        #construct the upload dir path
        log_dir = os.path.join(settings.UPLOAD_SERVER_PATH, communityFolder, 'upload_logs')

        if not os.path.exists(log_dir):
                os.mkdir(log_dir)

        import datetime
        now = datetime.datetime.now()
        log_file_name = os.path.join(log_dir, now.strftime("%Y-%m-%d-%H:%M:%S")) 

        if not os.path.exists(log_file_name):
                open(log_file_name, 'w').write(server_response)

#this is the default view
def default(request):

        applicationFolder = getApplicationFolder()

        #if the request is POST -> file is being uploaded
        if request.method == 'POST':
                form = UploadFileForm(request.POST, request.FILES)
                #if there is file for uploading
                if form.is_valid():
                        
                        #uplaod the file on the server
                        tarPath = handle_uploaded_file(request.FILES['file'])

                        reportMetaData = {}

                        #here check it could be zip or tar.gz
                        isTar = (tarPath.endswith('tar.gz') or tarPath.endswith('tgz'))
                        isZip = tarPath.endswith('.zip')

                        #get metadata depending on the archive
                        if isTar:
                                reportMetaData = getReportMetadataTar(tarPath)
                        elif isZip:
                                reportMetaData = getReportMetadataZip(tarPath)
                        
                        #create new Report object set its metadata and save it
                        report = Report()
                        report.username = reportMetaData['user']
                        report.ganga_version = reportMetaData['version']
                        report.job_uuid = reportMetaData['uuid']
                        report.date_uploaded = datetime.now()
                        report.save()

                        #save the report's mon links
                        monlinks = reportMetaData['monlinks']
                        
                        for key, value in monlinks.items():

                                monlink = MonitoringLink()
                                monlink.report = report
                                monlink.monitoring_link_name = key
                                monlink.monitoring_link_path = value            
                                monlink.save()
                        
                        #rename the uploaded file on the server with the report id 
                        if isTar:
                                rename_file(tarPath, report.id, 'tar')

                        elif isZip:
                                rename_file(tarPath, report.id, 'zip')

                        #sets the download path that will be shown to the user
                        serverDownloadPath = os.path.join(settings.SERVER_DOWNLOAD_PATH, str(report.id))

                        community, debug = getCommunityAndDebugMode()[0], getCommunityAndDebugMode()[1]
                        if community == 'CMS' and debug == False :
                                #because for CMS there is different DNS
                                serverDownloadPath = os.path.join("http://analysisops.cern.ch/cmserrorreports/download", str(report.id))
                        elif community == 'CMS':
                                serverDownloadPath = serverDownloadPath.replace('errorreports', 'cmserrorreports')

                        renderToResponse = render_to_response('%s/upload.html' % applicationFolder, {'form': form, 'path': serverDownloadPath }, context_instance = RequestContext(request))    

                        logServerResponse(str(renderToResponse))

                        return renderToResponse
                        
                else:    
                        return render_to_response('%s/upload.html' % applicationFolder, {'form': form }, context_instance = RequestContext(request))
        else:
                #user = User.objects.create_user('gosho', 'gosho@abv.bg', 'gosho')
                #user.save()

                return render_to_response('%s/upload.html' % applicationFolder, {}, context_instance = RequestContext(request))

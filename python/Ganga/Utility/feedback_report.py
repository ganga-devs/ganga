
def report(job=None):
        """ Upload error reports (snapshot of configuration,job parameters, input/output files, command history etc.). Job argument is optional. """
        import mimetypes
        import urllib2
        import httplib
        import string
        import random
        import sys
        import os
        import platform


        from Ganga.GPI import config
        from Ganga.GPI import full_print
        from Ganga.GPI import Job
        
        import Ganga

        #global variables that will print sumamry report to the user along with the download link
        global JOB_REPORT, GANGA_VERSION, BACKEND_NAME, APPLICATION_NAME, PYTHON_PATH
        JOB_REPORT = False
        GANGA_VERSION = ''
        BACKEND_NAME = ''
        APPLICATION_NAME = ''
        PYTHON_PATH = ''

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

                responseResult = responseResult[responseResult.find("<span id=\"download_path\""):]
                startIndex = responseResult.find("path:") + 5
                endIndex = responseResult.find("</span>")

                print 'Your error report was uploaded to ganga developers with the following URL. '
                print 'You may include this URL and the following summary information in your bug report or in the support email to the developers.'
                print
                print '***',responseResult[startIndex:endIndex],'***'
                print
                global GANGA_VERSION, JOB_REPORT, APPLICATION_NAME, BACKEND_NAME, PYTHON_PATH
                print 'Ganga Version : ' + GANGA_VERSION
                print 'Python Version : ' + "%s.%s.%s" % (sys.version_info[0], sys.version_info[1], sys.version_info[2])
                print 'Operation System Version : ' + platform.platform() 

                if JOB_REPORT:
                        print 'Application Name : ' + APPLICATION_NAME
                        print 'Backend Name : ' + BACKEND_NAME

                print 'Python Path : ' + PYTHON_PATH
                print

                JOB_REPORT = False
                GANGA_VERSION = ''
                BACKEND_NAME = ''
                APPLICATION_NAME = ''
                PYTHON_PATH = ''

        def run_upload (server, path):

                upload_file = make_upload_file (server)
                upload_file (path)
        

        def report_inner(job=None, isJob, isTask):
        
                userInfoDirName = "userreport"
                tempDirName = "reportsRepository"
                #job relevant info
                jobSummaryFileName = "jobsummary.txt"
                jobFullPrintFileName = "jobfullprint.txt"
                repositoryPath = "repository/$usr/LocalXML/6.0/jobs/$thousandsNumxxx"
                #task relevant info
                taskSummaryFileName = "tasksummary.txt"
                taskFullPrintFileName = "taskfullprint.txt"
                tasksRepositoryPath = "repository/$usr/LocalXML/6.0/tasks/$thousandsNumxxx"
                #non job relevant info - user's info
                environFileName = "environ.txt"
                userConfigFileName = "userconfig.txt"
                defaultConfigFileName = "gangarc.txt"
                ipythonHistoryFileName = "ipythonhistory.txt"
                gangaLogFileName = "gangalog.txt"
                jobsListFileName = "jobslist.txt"
                #uploadFileServer= "http://gangamon.cern.ch/django/errorreports/"
                uploadFileServer= "http://127.0.0.1:8000/errorreports"

                def printDictionary(dictionary):
                        for k,v in dictionary.iteritems():
                                print '%s: %s' % (k,v)
                                print
                                
                                if k == 'PYTHONPATH':
                                        global PYTHON_PATH
                                        PYTHON_PATH = v

                def extractFileObjects(fileName, targetDirectoryName):
                        try:
                                fileToRead = open(fileName, 'r')
                                try:
                                        fileText = fileToRead.read()
                                        import re
                                        pattern = "File\(name=\'(.+?)\'"
                                        matches = re.findall(pattern, fileText) 
                                        
                                        for fileName in matches:
                                                fileName = os.path.expanduser(fileName) 
                                                targetFileName = os.path.join(targetDirectoryName, os.path.basename(fileName))                                  
                                                shutil.copyfile(fileName, targetFileName)               

                                finally:
                                        fileToRead.close()
                        #except IOError, OSError:
                        except:
                                writeErrorLog(str(sys.exc_value))
        
                def writeErrorLog(errorMessage):
                        try:
                                fileToWrite = open(errorLogPath, 'a')
                                try:
                                        fileToWrite.write(errorMessage)
                                        fileToWrite.write("\n")
                                finally:
                                        fileToWrite.close()
                        except:
                                pass
                                

                def writeStringToFile(fileName, stringToWrite):

                        try:
                                #uncomment this to try the error logger
                                #fileName = '~/' + fileName
                                fileToWrite = open(fileName, 'w')
                                try:
                                        fileToWrite.write(stringToWrite)
                                finally:
                                        fileToWrite.close()
                        #except IOError:
                        except:
                                writeErrorLog(str(sys.exc_value))

                def renameDataFiles(directory):


                        for fileName in os.listdir(directory):
                                fullFileName = os.path.join(directory, fileName)
                                if os.path.isfile(fullFileName):
                                        if fileName == 'data':
                                                os.rename(fullFileName, fullFileName + '.txt')
                                else:
                                        renameDataFiles(fullFileName)   

                import shutil
                import tarfile
                import tempfile
                import os

                userHomeDir = os.getenv("HOME")
                tempDir = tempfile.mkdtemp()

                errorLogPath = os.path.join(tempDir, 'reportErrorLog.txt')

                fullPathTempDir = os.path.join(tempDir, tempDirName)
                fullLogDirName = ''
                #create temp dir and specific dir for the job/user

                try:
                        if not os.path.exists(fullPathTempDir):
                                os.mkdir(fullPathTempDir)

                        import datetime
                        now = datetime.datetime.now()
                        userInfoDirName = userInfoDirName + now.strftime("%Y-%m-%d-%H:%M:%S")
                        fullLogDirName = os.path.join(fullPathTempDir, userInfoDirName) 
        
                        #if report directory exists -> delete it's content(we would like last version of the report)
                        if os.path.exists(fullLogDirName):
                                shutil.rmtree(fullLogDirName)
        
                        os.mkdir(fullLogDirName)
                #except OSError:
                except:
                        writeErrorLog(str(sys.exc_value))

                #import os.environ in a file
                fullEnvironFileName = os.path.join(fullLogDirName, environFileName)

                try:
                        inputFile = open(fullEnvironFileName, 'w')
                        try:
                                sys.stdout = inputFile
                                printDictionary(os.environ)

                                print 'OS VERSION : ' + platform.platform()

                        finally:
                                sys.stdout = sys.__stdout__
                                inputFile.close()       
                #except IOError         
                except:
                        writeErrorLog(str(sys.exc_value))

                #import user config in a file   
                userConfigFullFileName = os.path.join(fullLogDirName, userConfigFileName)
                
                try:
                        inputFile = open(userConfigFullFileName, 'w')
                        try:
                        
                                sys.stdout = inputFile

                                print "#GANGA_VERSION = %s" % config.System.GANGA_VERSION 
                                print

                                global GANGA_VERSION
                                GANGA_VERSION = config.System.GANGA_VERSION

                                #this gets the default values
                                #Ganga.GPIDev.Lib.Config.Config.print_config_file()
                                
                                #this should get the changed values
                                for c in config:
                                        print config[c]

                        finally:
                                sys.stdout = sys.__stdout__
                                inputFile.close()
                #except IOError does not catch the exception ???
                except:
                        writeErrorLog(str(sys.exc_value))

                #write gangarc - default configuration
                defaultConfigFullFileName = os.path.join(fullLogDirName, defaultConfigFileName)

                try:
                        outputFile = open(os.path.join(userHomeDir, '.gangarc'), 'r')
        
                        try:
                                writeStringToFile(defaultConfigFullFileName, outputFile.read())  
                        finally:
                                outputFile.close()
                                        
                #except IOError does not catch the exception ???                
                except:
                        writeErrorLog(str(sys.exc_value))

                #import ipython history in a file
                try:
                        ipythonFile = open(os.path.join(os.environ['IPYTHONDIR'], 'history'), 'r')
                        
                        try:                    
                                lastIPythonCommands = ipythonFile.readlines()[-20:]             
                                writeStringToFile(os.path.join(fullLogDirName, ipythonHistoryFileName), '\n'.join(lastIPythonCommands))
                                #writeStringToFile(os.path.join(fullLogDirName, ipythonHistoryFileName), ipythonFile.read())
                        finally:
                                ipythonFile.close()
                #except IOError does not catch the exception ???                
                except:
                        writeErrorLog(str(sys.exc_value))

                #import gangalog in a file      
                userLogFileLocation = config["Logging"]._logfile
                userLogFileLocation = os.path.expanduser(userLogFileLocation)

                try:
                        gangaLogFile = open(userLogFileLocation, 'r')
                        try:            
                                writeStringToFile(os.path.join(fullLogDirName, gangaLogFileName), gangaLogFile.read())  
                        finally:
                                gangaLogFile.close()
                #except IOError:
                except:
                        writeErrorLog(str(sys.exc_value))

                #import the result of jobs command in the report
                jobsListFullFileName = os.path.join(fullLogDirName, jobsListFileName)
                
                try:
                        outputFile = open(jobsListFullFileName, 'w')
                        try:
                        
                                sys.stdout = outputFile
                                
                                from Ganga.GPI import jobs
                                print jobs      
                                        
                        finally:
                                sys.stdout = sys.__stdout__
                                outputFile.close()
                                        
                #except IOError does not catch the exception ???                
                except:
                        writeErrorLog(str(sys.exc_value))
        
                #save it here because we will change fullLogDirName, but we want this to be the archive and to be deleted 
                folderToArchive = fullLogDirName

                #import job relevant info
                if (job is not None and isJob):

                        global JOB_REPORT, APPLICATION_NAME, BACKEND_NAME

                        JOB_REPORT = True
                        APPLICATION_NAME = job.application.__class__.__name__
                        BACKEND_NAME = job.backend.__class__.__name__


                        #create job folder                      
                        jobFolder = 'job_%s' % str(job.fqid)
                        fullLogDirName = os.path.join(fullLogDirName, jobFolder)
                        os.mkdir(fullLogDirName)                        

                        #import job summary in a file   
                        fullJobSummaryFileName = os.path.join(fullLogDirName, jobSummaryFileName)
                        writeStringToFile(fullJobSummaryFileName, str(job))

                        #import job full print in a file
                        fullJobPrintFileName = os.path.join(fullLogDirName, jobFullPrintFileName)
                        
                        try:
                                inputFile = open(fullJobPrintFileName, 'w')
                                try:
                                        full_print(job, inputFile)
                                finally:
                                        inputFile.close()
                        #except IOError, OSError:
                        except:
                                writeErrorLog(str(sys.exc_value))

                        #extract file objects
                        try:                    
                                fileObjectsPath = os.path.join(fullLogDirName, 'fileobjects')
                                os.mkdir(fileObjectsPath)
                                extractFileObjects(fullJobSummaryFileName, fileObjectsPath)
                        #except OSError:
                        except:
                                writeErrorLog(str(sys.exc_value))

                        #copy dir of the job ->input/output and subjobs
                        try:
                                parentDir, currentDir = os.path.split(job.inputdir[:-1])
                                workspaceDir = os.path.join(fullLogDirName, 'workspace')
                                shutil.copytree(parentDir, workspaceDir)
                        #except IOError, OSError
                        except:
                                writeErrorLog(str(sys.exc_value))

                        #copy shared area of the job 
                        try:

                                if hasattr(job.application,'is_prepared'):
                                        if job.application.is_prepared is not None and job.application.is_prepared is not True:
                                                import os
                                                from Ganga.Utility.Config import getConfig
                                                from Ganga.Utility.files import expandfilename
                                                shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
                                                shareddir = os.path.join(shared_path,job.application.is_prepared.name)
                                                if os.path.isdir(shareddir):

                                                        sharedAreaDir = os.path.join(fullLogDirName, 'sharedarea')
                                                        shutil.copytree(shareddir, sharedAreaDir)
                        #except IOError, OSError
                        except:
                                writeErrorLog(str(sys.exc_value))

                        #copy repository job file
                        try:
                                indexFileName = str(job.id) + '.index'

                                repositoryPath = repositoryPath.replace('$usr', os.getenv("USER"))
                                
                                #check if the job is subjob -> different way of forming the path to the repository
                                is_subjob = job.fqid.find('.') > -1

                                if is_subjob:

                                        jobid, subjobid = job.fqid.split('.')[0], job.fqid.split('.')[1]
                                        repositoryPath = repositoryPath.replace('$thousandsNum', str(int(jobid)/1000))
                                        repositoryPath = os.path.join(repositoryPath, jobid)

                                else:   
                                        repositoryPath = repositoryPath.replace('$thousandsNum', str(job.id/1000))

                                repositoryFullPath = os.path.join(config.Configuration.gangadir, repositoryPath)
                                indexFileSourcePath = os.path.join(repositoryFullPath, indexFileName)
                                repositoryFullPath = os.path.join(repositoryFullPath, str(job.id))

                                repositoryTargetPath = os.path.join(fullLogDirName, 'repository', str(job.id))
                                
                                os.mkdir(os.path.join(fullLogDirName, 'repository'))

                                shutil.copytree(repositoryFullPath, repositoryTargetPath)
                                #data files are copied but can not be opened -> add .txt to their file names
                                renameDataFiles(repositoryTargetPath)

                                if not is_subjob:
                                        #copy .index file
                                        indexFileTargetPath = os.path.join(fullLogDirName, 'repository', indexFileName)
                                        shutil.copyfile(indexFileSourcePath, indexFileTargetPath)
                                
                        #except OSError, IOError:
                        except:
                                writeErrorLog(str(sys.exc_value))


                #import task relevant info
                if (job is not None and isTask):
                #job is actually a task object
                        task = job
                        #create task folder                      
                        taskFolder = 'task_%s' % str(task.id)
                        fullLogDirName = os.path.join(fullLogDirName, taskFolder)
                        os.mkdir(fullLogDirName)                        

                        #import task summary in a file   
                        fullTaskSummaryFileName = os.path.join(fullLogDirName, taskSummaryFileName)
                        writeStringToFile(fullTaskSummaryFileName, str(task))

                        #import task full print in a file
                        fullTaskPrintFileName = os.path.join(fullLogDirName, taskFullPrintFileName)
                        
                        try:
                                inputFile = open(fullTaskPrintFileName, 'w')
                                try:
                                        full_print(task, inputFile)
                                finally:
                                        inputFile.close()
                        #except IOError, OSError:
                        except:
                                writeErrorLog(str(sys.exc_value))

                        #copy shared area of the task 
                        try:
                                if len(task.transforms) > 0:
                                        if hasattr(task.transforms[0], 'application') and hasattr(task.transforms[0].application, 'is_prepared'):
                                                if task.transforms[0].application.is_prepared is not None and task.transforms[0].application.is_prepared is not True:
                                                        import os
                                                        from Ganga.Utility.Config import getConfig
                                                        from Ganga.Utility.files import expandfilename
                                                        shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
                                                        shareddir = os.path.join(shared_path,task.transforms[0].application.is_prepared.name)
                                                        if os.path.isdir(shareddir):

                                                                sharedAreaDir = os.path.join(fullLogDirName, 'sharedarea')
                                                                shutil.copytree(shareddir, sharedAreaDir)
                        #except IOError, OSError
                        except:
                                writeErrorLog(str(sys.exc_value))


                        #copy repository task file
                        try:
                                indexFileName = str(task.id) + '.index'

                                tasksRepositoryPath = tasksRepositoryPath.replace('$usr', os.getenv("USER"))
                                tasksRepositoryPath = tasksRepositoryPath.replace('$thousandsNum', str(task.id/1000))

                                repositoryFullPath = os.path.join(config.Configuration.gangadir, tasksRepositoryPath)
                                indexFileSourcePath = os.path.join(repositoryFullPath, indexFileName)
                                repositoryFullPath = os.path.join(repositoryFullPath, str(task.id))

                                repositoryTargetPath = os.path.join(fullLogDirName, 'repository', str(task.id))
                                
                                os.mkdir(os.path.join(fullLogDirName, 'repository'))

                                shutil.copytree(repositoryFullPath, repositoryTargetPath)
                                #data files are copied but can not be opened -> add .txt to their file names
                                renameDataFiles(repositoryTargetPath)

                                #copy .index file
                                indexFileTargetPath = os.path.join(fullLogDirName, 'repository', indexFileName)
                                shutil.copyfile(indexFileSourcePath, indexFileTargetPath)
                                
                        #except OSError, IOError:
                        except:
                                writeErrorLog(str(sys.exc_value))


                print folderToArchive

                """
                resultArchive = '%s.tar.gz' % folderToArchive

                try:
                        resultFile = tarfile.TarFile.open(resultArchive, 'w:gz')
                        try:
                                resultFile.add(folderToArchive, arcname=os.path.basename(folderToArchive))
                                #put the error log in the archive
                                if(os.path.exists(errorLogPath)):
                                        resultFile.add(errorLogPath, arcname=os.path.basename(errorLogPath))    
                
                        finally:        
                                resultFile.close()
                except:
                        raise #pass
                
                #remove temp dir
                if(os.path.exists(folderToArchive)):
                        shutil.rmtree(folderToArchive)
                """

                #print the error if there is something
                if os.path.exists(errorLogPath):
                        print
                        print 'An error occured while collecting report information : ' +  open(errorLogPath, 'r').read()
                        print  

                #delete the errorfile from user's pc
                if(os.path.exists(errorLogPath)):
                        os.remove(errorLogPath)

                #return the path to the archive and the path to the upload server
                return (resultArchive, uploadFileServer, tempDir)

        def removeTempFiles(tempDir):
                import shutil
                
                #remove temp dir
                if os.path.exists(tempDir):
                        shutil.rmtree(tempDir)  

                #remove temp files from django upload-> if the file is bigger than 2.5 mb django internally stores it in tmp file during the upload
                userTempDir = '/tmp/'
                
                for fileName in os.listdir(userTempDir):
                        if fileName.find('.upload') > -1:
                                os.remove(os.path.join(userTempDir, fileName)) 


        tempDir = ''

        #call the report function
        try:

                #make typecheck of the param passed             
                if job is not None:
                        isJob = isinstance(job,Job)
                        isTask = (job._impl._category == 'tasks')

                        if not (isJob or isTask):
                                print "report() function argument should be reference to a job or task object"
                                return

                resultArchive, uploadFileServer, tempDir = report_inner(job, isJob, isTask)

                report_bytes = os.path.getsize(resultArchive)
                
                if report_bytes > 1024*1024*100: #if bigger than 100MB
                        print 'The report is bigger than 100MB and can not be uploaded'
                else:
                        run_upload(server=uploadFileServer, path=resultArchive)

                
        except:
                removeTempFiles(tempDir)
                raise #pass
                #raise  

        removeTempFiles(tempDir)

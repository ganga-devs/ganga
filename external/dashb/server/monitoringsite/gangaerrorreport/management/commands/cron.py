from django.core.management.base import BaseCommand
from monitoringsite.gangaerrorreport.models import Report
from monitoringsite import settings
from operator import itemgetter

import os

COMMUNITY = 'Ganga'
#COMMUNITY = 'CMS'
MAX_FOLDER_SIZE = 1000000 # in bytes

def getFolderSize(folderPath):

        folder_size = 0

        for (path, dirs, files) in os.walk(folderPath):
                for file in files:
                        filename = os.path.join(path, file)
                        folder_size += os.path.getsize(filename)

        return folder_size

def cleanOldReports(reportsFolder):
        
        file_dictionary = {}
        
        for reportFile in os.listdir(reportsFolder):
                try:
                        reportid = int(reportFile[:reportFile.find('.')])
                        fullFileName = os.path.join(reportsFolder, reportFile)
                        #the value should be the id of the report
                        file_dictionary[fullFileName] = reportid
                except ValueError:
                        pass

        #sort the dict by value - id of report - the first are the oldest
        sorted = file_dictionary.items()
        sorted.sort(key = itemgetter(1))

        for i in range(len(sorted)):

                try:
                        os.remove(sorted[i][0])
                except OSError:
                        pass
                
                try:
                        report = Report.objects.get(id=sorted[i][1])
                        report.is_deleted = True
                        report.save()
                except:
                        pass


                folderSize = getFolderSize(reportsFolder)

                if folderSize <= MAX_FOLDER_SIZE:
                        break

class Command(BaseCommand):
        
        def handle(self, *args, **options):

                reportsFolder = os.path.join(settings.UPLOAD_SERVER_PATH, COMMUNITY)

                folderSize = getFolderSize(reportsFolder)

                print 'folder size ' + str(folderSize)

                if folderSize > MAX_FOLDER_SIZE:
                        cleanOldReports(reportsFolder)

                reports = Report.objects.order_by('-id').all()

                print 'number of reports : %s' % len(reports)

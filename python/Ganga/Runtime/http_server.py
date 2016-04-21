from BaseHTTPServer import BaseHTTPRequestHandler
from Ganga.Core.GangaRepository import getRegistry, RegistryKeyError
from Ganga.Core.GangaThread import GangaThread
from Ganga.Utility.util import hostname
from Ganga.GPIDev.Base.Proxy import getName
from BaseHTTPServer import HTTPServer

import urlparse
import Ganga.GPI
from Ganga.GPIDev.Lib.Config import config
import time
import datetime
import os
logger = Ganga.Utility.logging.getLogger()

job_status_color = {'new': '00FFFF',
                    'submitting': 'FFFFFF',
                    'submitted': '0000FF',
                    'running': '008000',
                    'completed': '006400',
                    'completing': '006400',
                    'killed': 'FF0000',
                    'unknown': '808080',
                    'incomplete': 'FF00FF',
                    'failed': 'FF0000'}


subjob_status_color = {'new': '00ff7d',
                       'submitting': 'FFFFFF',
                       'submitted': '00007d',
                       'running': '00f000',
                       'completed': '009000',
                       'completing': '009000',
                       'killed': '7d0000',
                       'unknown': '808080',
                       'incomplete': '7d007d',
                       'failed': '7D0000'}


def getColorString(statuses, jobs=True):

    colorString = ""
    colorDictionary = job_status_color

    if not jobs:
        colorDictionary = subjob_status_color

    for status in statuses[:-1]:
        colorString += '%s|' % colorDictionary[status]
    colorString += colorDictionary[statuses[-1]]

    return colorString


def addQuotes(value):

    trimmedValue = value.strip('\'')

    return '"' + trimmedValue + '"'


def get_users_JSON():

    json_users = "{\"basicData\": [[{\"GridName\": \"%s\"}]]}" % config.Configuration.user

    return json_users


def get_subjob_JSON(job):

    result = []
    result.append("{")

    result.append("\"id\": %s," % addQuotes(job.fqid))
    result.append("\"status\": %s," % addQuotes(job.status))
    result.append("\"name\": %s," % addQuotes(job.name))
    result.append("\"application\": %s," %
                  addQuotes(getName(job.application)))
    result.append("\"backend\": %s," %
                  addQuotes(getName(job.backend)))
    result.append("\"actualCE\": %s" % addQuotes(job.backend.actualCE))

    result.append("}")

    return "".join(result)


def get_job_JSON(job):

    undefinedAttribute = 'UNDEFINED'

    result = []
    result.append("{")

    result.append("\"id\": %s," % addQuotes(job.fqid))
    result.append("\"status\": %s," % addQuotes(job.status))
    result.append("\"name\": %s," % addQuotes(job.name))

    # add mon links in the JSON
    mon_links = job.info.monitoring_links
    mon_links_html = ''

    if len(mon_links) > 0:
        number = 1
        for mon_link in mon_links:
            # if it is string -> just the path to the link
            if isinstance(mon_link, str):
                mon_links_html = mon_links_html + \
                    '<div>&nbsp;&nbsp;&nbsp;<a href=\'%s\'>mon_link_%s</a></div>' % (
                        mon_link, number)
                number += 1
            elif isinstance(mon_link, tuple):
                if len(mon_link) == 2:
                    mon_links_html = mon_links_html + \
                        '<div>&nbsp;&nbsp;&nbsp;<a href=\'%s\'>%s</a></div>' % (
                            mon_link[0], mon_link[1])
                else:
                    mon_links_html = mon_links_html + \
                        '<div>&nbsp;&nbsp;&nbsp;<a href=\'%s\'>mon_link_%s</a></div>' % (
                            mon_link[0], number)
                    number += 1

    result.append("\"link\": %s," % addQuotes(mon_links_html))

    # test for expandable data
    result.append("\"inputdir\": %s," % addQuotes(job.inputdir))
    result.append("\"outputdir\": %s," % addQuotes(job.outputdir))

    try:
        result.append("\"submitted\": %s," % addQuotes(
            str(len(job.subjobs.select(status='submitted')))))
        result.append("\"running\": %s," %
                      addQuotes(str(len(job.subjobs.select(status='running')))))
        result.append("\"completed\": %s," % addQuotes(
            str(len(job.subjobs.select(status='completed')))))
        result.append("\"failed\": %s," %
                      addQuotes(str(len(job.subjobs.select(status='failed')))))

        result.append("\"application\": %s," %
                      addQuotes(getName(job.application)))
        result.append("\"backend\": %s," %
                      addQuotes(getName(job.backend)))
        result.append("\"subjobs\": %s," % addQuotes(str(len(job.subjobs))))
        result.append("\"uuid\": %s," % addQuotes(job.info.uuid))

        try:
            result.append("\"actualCE\": %s," %
                          addQuotes(job.backend.actualCE))
        except AttributeError:
            result.append("\"actualCE\": %s," % addQuotes(undefinedAttribute))

    except RegistryKeyError:
        pass

    # remove the last , -> else invalid JSON
    if result[len(result) - 1][-1] == ',':
        last = result[-1]
        result = result[:-1]
        result.append(last[:-1])

    result.append("}")

    return "".join(result)


def get_subjobs_in_time_range(jobid, fromDate=None, toDate=None):
    from Ganga.Core.GangaRepository import getRegistryProxy

    subjobs = []

    for subjob in getRegistryProxy('jobs')(jobid).subjobs:

        timeCreated = subjob.time.timestamps['new']

        if fromDate is None and toDate is None:

            subjobs.append(subjob)

        elif fromDate is not None and toDate is not None:

            if timeCreated >= fromDate and timeCreated <= toDate:

                subjobs.append(subjob)

        elif fromDate is not None and toDate is None:

            if timeCreated >= fromDate:

                subjobs.append(subjob)

    return subjobs


def get_subjobs_JSON(jobid, fromDate=None, toDate=None):

    json_subjobs_strings = []
    json_subjobs_strings.append("{\"taskjobs\": [")

    subjobs_in_time_range = get_subjobs_in_time_range(jobid, fromDate, toDate)

    for subjob in subjobs_in_time_range:

        json_subjobs_strings.append(get_subjob_JSON(subjob))
        json_subjobs_strings.append(",")

    if json_subjobs_strings[-1] == ",":
        json_subjobs_strings = json_subjobs_strings[:-1]

    json_subjobs_strings.append("]}")

    return "".join(json_subjobs_strings)


def get_job_infos_in_time_range(fromDate=None, toDate=None):

    job_infos = []

    for jobInfo in jobs_dictionary.values():

        timeCreated = jobInfo.getTimeCreated()

        if timeCreated is None:

            if fromDate is None and toDate is None:

                job_infos.append(jobInfo)

        elif fromDate is None and toDate is None:

            job_infos.append(jobInfo)

        elif fromDate is not None and toDate is not None:

            if timeCreated >= fromDate and timeCreated <= toDate:

                job_infos.append(jobInfo)

        elif fromDate is not None and toDate is None:

            if timeCreated >= fromDate:

                job_infos.append(jobInfo)

    return job_infos

# increment dictionary value method


def increment(d, k):
    d.setdefault(k, 0)
    d[k] += 1


def get_accumulated_subjobs_JSON(subjobs):

    completed_dates = []

    for subjob in subjobs:
        if subjob.status == 'completed':
            completed_dates.append(subjob.time.timestamps['final'])

    if len(completed_dates) == 0:
        return ''

    completed_dates.sort()

    start_date = completed_dates[0]
    end_date = completed_dates[-1]

    interval = (end_date - start_date).seconds

    if interval == 0:
        interval = 1

    ratio = 100.0 / interval

    seconds_from_start = []

    scale = len(completed_dates) / 20

    for completed_date in completed_dates:
        seconds_from_start.append(
            ((completed_date - start_date).seconds) * ratio)

    values = []
    for i in range(len(completed_dates)):
        values.append(i + 1)

    reduced_values = []
    reduced_seconds_from_start = []

    ratio1 = 100.0 / len(completed_dates)

    for i in range(len(values)):
        if scale == 0:
            reduced_values.append(str(values[i] * ratio1))
            reduced_seconds_from_start.append(str(seconds_from_start[i]))
        elif (i % scale == 0):
            reduced_values.append(str(values[i] * ratio1))
            reduced_seconds_from_start.append(str(seconds_from_start[i]))

    reduced_values.append(str(values[-1] * ratio1))
    reduced_seconds_from_start.append(str(seconds_from_start[-1]))

    if interval == 1:
        reduced_values = ["0", "100"]
        reduced_seconds_from_start = ["0", "100"]

    returnJSON = "{\"chxl\":\"0:|" + start_date.strftime("%Y-%m-%d %H:%M:%S") + "|" + end_date.strftime("%Y-%m-%d %H:%M:%S") + "\"," + "\"chd\":\"t:" + ','.join(
        reduced_seconds_from_start) + "|" + ','.join(reduced_values) + "\",\"chxr\":\"1,0," + str(values[-1]) + "\"}"

    return returnJSON


def create_subjobs_graphics(jobid, subjob_attribute, fromDate, toDate):

    subjobs_in_time_range = get_subjobs_in_time_range(jobid, fromDate, toDate)

    if subjob_attribute == 'accumulate':
        # return some JSON here
        return get_accumulated_subjobs_JSON(subjobs_in_time_range)

    subjobs_attributes = {}

    for subjob in subjobs_in_time_range:

        if subjob_attribute == 'status':
            increment(subjobs_attributes, subjob.status)

        elif subjob_attribute == 'application':
            increment(
                subjobs_attributes, getName(subjob.application))

        elif subjob_attribute == 'backend':
            increment(subjobs_attributes, getName(subjob.backend))

        elif subjob_attribute == 'actualCE':
            increment(subjobs_attributes, subjob.backend.actualCE)

    if subjob_attribute == 'status':
        return get_pie_chart_json(subjobs_attributes, colors=True, jobs=False)
    else:
        return get_pie_chart_json(subjobs_attributes)


def get_pie_chart_json(d, colors=False, jobs=False):

    #template = "{\"chd\":\"t:50,50\",\"chl\":\"Hello|World\"}"

    if len(d) == 0:
        return "{\"chd\":\"t:1\",\"chl\":\"no data\"}"

    keys = []
    values = []

    for k, v in d.iteritems():
        keys.append(k)
        values.append(v)

    keyString = ""

    for key in keys[:-1]:
        keyString += '%s|' % key
    keyString += keys[-1]

    valueString = ""

    for value in values[:-1]:
        valueString += '%s,' % str(value)
    valueString += str(values[-1])

    result_json = ""

    if colors:
        colorString = getColorString(keys, jobs)
        result_json = "{\"chd\":\"t:%s\",\"chl\":\"%s\",\"chco\":\"%s\"}" % (
            valueString, keyString, colorString)
    else:
        result_json = "{\"chd\":\"t:%s\",\"chl\":\"%s\"}" % (
            valueString, keyString)

    return result_json


def create_jobs_graphics(job_attribute, fromDate=None, toDate=None):

    job_infos_in_time_range = get_job_infos_in_time_range(fromDate, toDate)

    jobs_attribute = {}

    for jobInfo in job_infos_in_time_range:

        if job_attribute == 'status':
            increment(jobs_attribute, jobInfo.getJobStatus())
        elif job_attribute == 'application':
            increment(jobs_attribute, jobInfo.getJobApplication())
        elif job_attribute == 'backend':
            increment(jobs_attribute, jobInfo.getJobBackend())

    if job_attribute == 'status':
        return get_pie_chart_json(jobs_attribute, colors=True, jobs=True)
    else:
        return get_pie_chart_json(jobs_attribute)


def get_jobs_JSON(fromDate=None, toDate=None):

    json_jobs_strings = []
    json_jobs_strings.append("{\"user_taskstable\": [")

    job_infos_in_time_range = get_job_infos_in_time_range(fromDate, toDate)

    for jobInfo in job_infos_in_time_range:

        json_jobs_strings.append(jobInfo.getJobJSON())
        json_jobs_strings.append(",")

    if json_jobs_strings[-1] == ",":
        json_jobs_strings = json_jobs_strings[:-1]

    json_jobs_strings.append("]}")

    return "".join(json_jobs_strings)


def update_jobs_dictionary():
    from Ganga.Core.GangaRepository import getRegistryProxy

    reg = getRegistry("jobs")
    # get the changed jobs
    changed_ids = reg.pollChangedJobs("WebGUI")

    for job_id in changed_ids:
        try:

            job = getRegistryProxy('jobs')(job_id)

            try:
                jobs_dictionary[job_id] = JobRelatedInfo(
                    job, job.time.timestamps['new'])
            except RegistryKeyError:
                jobs_dictionary[job_id] = JobRelatedInfo(job, None)

        except RegistryKeyError:

            del jobs_dictionary[job_id]


def fill_jobs_dictionary():
    from Ganga.Core.GangaRepository import getRegistryProxy

    for job in getRegistryProxy('jobs'):
        try:
            # get the id -> it could cause RegistryKeyError and the code below
            # will not be executed
            jobid = job.id

            try:
                jobs_dictionary[jobid] = JobRelatedInfo(
                    job, job.time.timestamps['new'])
            except RegistryKeyError:
                jobs_dictionary[jobid] = JobRelatedInfo(job, None)

        except RegistryKeyError:
            pass

# todo remove


def saveProcessDetails():

    if not os.path.exists(tempFilePath):
        file = open(tempFilePath, 'w')
        try:
            file.write(str(os.getpid()))
            file.write('\n')
            file.write(Ganga.Utility.util.hostname())
        finally:
            file.close()
# todo remove


def getProcessDetails():

    file = open(tempFilePath, 'r')
    try:
        lines = file.readlines()
        pid = lines[0].strip()
        hostname = lines[1].strip()

        return (pid, hostname)
    finally:
        file.close()


def getHttpServer():

    success = False
    port = httpServerStartTryPort
    server = None

    while not success:

        try:
            server = HTTPServer((httpServerHost, port), GetHandler)
            success = True
        except Exception:
            port += 1

    return server, port


def convertStringToDatetime(timestring):

    time_format = "%Y-%m-%d %H:%M"
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(timestring, time_format)))


def getFromDateFromTimeRange(timeRange):

    fromDay = None
    today = datetime.date.today()

    if timeRange == 'lastDay':
        fromDay = today
    elif timeRange == 'last2Days':
        fromDay = today + datetime.timedelta(days=-1)
    elif timeRange == 'last3Days':
        fromDay = today + datetime.timedelta(days=-2)
    elif timeRange == 'lastWeek':
        fromDay = today + datetime.timedelta(days=-6)
    elif timeRange == 'last2Weeks':
        fromDay = today + datetime.timedelta(days=-13)
    elif timeRange == 'lastMonth':
        fromDay = today + datetime.timedelta(days=-30)

    fromDateTime = datetime.datetime(
        fromDay.year, fromDay.month, fromDay.day, 0, 0)
    return fromDateTime


def getMonitoringLink(port):

    webMonitoringLink = os.path.join(config['System'][
                                     'GANGA_PYTHONPATH'], 'Ganga', 'Core', 'WebMonitoringGUI', 'client', 'index.html')

    return 'file://' + webMonitoringLink + '?port=' + str(port) + '#user=' + config.Configuration.user + '&timeRange='


class JobRelatedInfo:

    def __init__(self, job, time_created):

        self.job_json = get_job_JSON(job)
        self.time_created = time_created
        self.job_status = job.status
        self.job_application = getName(job.application)
        self.job_backend = getName(job.backend)

    def getJobJSON(self):

        return self.job_json

    def getTimeCreated(self):

        return self.time_created

    def getJobStatus(self):

        return self.job_status

    def getJobApplication(self):

        return self.job_application

    def getJobBackend(self):

        return self.job_backend

    def __hash__(self):

        return hash(self.job_json) + hash(self.time_created)

    def __eq__(self, other):

        return isinstance(other, JobRelatedInfo) and self.job_json == other.job_json and self.time_created == other.time_created


class HTTPServerThread(GangaThread):

    def __init__(self, name):
        GangaThread.__init__(self, name=name)

    def run(self):

        server, port = getHttpServer()
        server.socket.settimeout(1)
        reg = getRegistry("jobs")

        """
        try:
            server = HTTPServer(('pclcg35.cern.ch', 1234), GetHandler)
            server.socket.settimeout(1)
        except Exception:
            return
        
            print("Another Ganga session is already started with --webgui option" )      
            process_details = getProcessDetails()

            print(reg.repository.get_other_sessions())
            print("Process id : %s, hostname : %s" % (process_details[0], process_details[1]))
            
            self.stop()
            self.unregister()   
            return      
        """

        logger.info('Starting web gui monitoring server, please wait ...')

        #   initialization

        reg = getRegistry("jobs")
        # calling here first time will take all jobs
        reg.pollChangedJobs("WebGUI")
        # fill jobs dictionary at the begining
        fill_jobs_dictionary()

        logger.info('Web gui monitoring server started successfully')
        logger.info('You can monitor your jobs at the following location: ' + getMonitoringLink(port))

        # server.serve_forever()

        try:
            while not self.should_stop():
                server.handle_request()
        finally:
            pass
            #print("stopping HTTP server thread")
            # os.remove(tempFilePath)
            # server.server_close()


class GetHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):

        logger.debug(format % args)

    def do_GET(self):
        queryString = self.path.split('?')[1]
        import cgi
        qsDict = dict(cgi.parse_qsl(queryString))
        query = qsDict['list']

        fromDate = None
        toDate = None

        # from and to date are either both selected or both not selected
        if 'from' in qsDict and 'to' in qsDict:
            fromDate = convertStringToDatetime(qsDict['from'])
            toDate = convertStringToDatetime(qsDict['to'])
        # if from and to are not selected, it could be timeRange selected
        elif 'timerange' in qsDict:
            fromDate = getFromDateFromTimeRange(qsDict['timerange'])

        json = ''

        if query == "users":
            json = get_users_JSON()
        elif query == "jobs":
            # update dictionary with the changed jobs
            update_jobs_dictionary()
            json = get_jobs_JSON(fromDate, toDate)

        elif query == "subjobs":
            jobid = int(qsDict['taskmonid'])
            json = get_subjobs_JSON(jobid, fromDate, toDate)

        elif query == "jobs_statuses":
            # update dictionary with the changed jobs
            update_jobs_dictionary()
            json = create_jobs_graphics('status', fromDate, toDate)

        elif query == "jobs_backends":
            # update dictionary with the changed jobs
            update_jobs_dictionary()
            json = create_jobs_graphics('backend', fromDate, toDate)

        elif query == "jobs_applications":
            # update dictionary with the changed jobs
            update_jobs_dictionary()
            json = create_jobs_graphics('application', fromDate, toDate)

        elif query == "subjobs_statuses":
            jobid = int(qsDict['taskmonid'])
            json = create_subjobs_graphics(jobid, 'status', fromDate, toDate)

        elif query == "subjobs_backends":
            jobid = int(qsDict['taskmonid'])
            json = create_subjobs_graphics(jobid, 'backend', fromDate, toDate)

        elif query == "subjobs_applications":
            jobid = int(qsDict['taskmonid'])
            json = create_subjobs_graphics(
                jobid, 'application', fromDate, toDate)

        elif query == "subjobs_actualCE":
            jobid = int(qsDict['taskmonid'])
            json = create_subjobs_graphics(jobid, 'actualCE', fromDate, toDate)

        elif query == "subjobs_accumulate":
            jobid = int(qsDict['taskmonid'])
            json = create_subjobs_graphics(
                jobid, 'accumulate', fromDate, toDate)

        elif query == "testaccumulation":

            json = "{\"totaljobs\": [[{\"TOTAL\": 92}], {\"taskmonid\": \"ganga:e60e5904-e63e-432f-b3df-63ca833cf080:\"}], \"procevents\": [[{\"NEventsPerJob\": 0}], {\"taskmonid\": \"ganga:e60e5904-e63e-432f-b3df-63ca833cf080:\"}], \"succjobs\": [[{\"TOTAL\": 92, \"TOTALEVENTS\": 1365491}], {\"taskmonid\": \"ganga:e60e5904-e63e-432f-b3df-63ca833cf080:\"}], \"meta\": {\"genactivity\": null, \"submissiontype\": null, \"site\": null, \"ce\": null, \"dataset\": null, \"submissiontool\": null, \"fail\": null, \"check\": [\"submitted\"], \"date1\": [\"2010-09-23 15:56:27\"], \"date2\": [\"2010-09-24 15:56:27\"], \"application\": null, \"rb\": null, \"status\": null, \"taskmonid\": [\"ganga:e60e5904-e63e-432f-b3df-63ca833cf080:\"], \"args\": \"<![CDATA[taskmonid=ganga%3Ae60e5904-e63e-432f-b3df-63ca833cf080%3A]]>\", \"grid\": null, \"user\": null, \"task\": null, \"unixname\": null, \"sortby\": [\"activity\"], \"activity\": null, \"exitcode\": null}, \"allfinished\": [[{\"finished\": \"2010-08-13 14:02:18\", \"Events\": 2000}, {\"finished\": \"2010-08-13 14:39:13\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:39:25\", \"Events\": 14350}, {\"finished\": \"2010-08-13 14:39:58\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:40:03\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:40:18\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:40:19\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:40:37\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:40:38\", \"Events\": 14994}, {\"finished\": \"2010-08-13 14:40:52\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:40:53\", \"Events\": 14996}, {\"finished\": \"2010-08-13 14:40:54\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:41:25\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:41:27\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:41:29\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:41:32\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:41:32\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:41:34\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:41:35\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:41:43\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:41:44\", \"Events\": 14996}, {\"finished\": \"2010-08-13 14:41:45\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:41:53\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:41:54\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:41:55\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:41:55\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:41:55\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:41:55\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:41:55\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:41:59\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:42:03\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:42:03\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:42:04\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:42:06\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:42:07\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:42:14\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:42:14\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:42:27\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:42:27\", \"Events\": 14995}, {\"finished\": \"2010-08-13 14:42:28\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:42:38\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:42:53\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:42:54\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:42:57\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:42:57\", \"Events\": 14995}, {\"finished\": \"2010-08-13 14:42:58\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:42:58\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:43:01\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:02\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:04\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:04\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:11\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:15\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:15\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:43:17\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:22\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:23\", \"Events\": 14996}, {\"finished\": \"2010-08-13 14:43:24\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:25\", \"Events\": 14996}, {\"finished\": \"2010-08-13 14:43:28\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:43:32\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:36\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:43:36\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:43:39\", \"Events\": 14996}, {\"finished\": \"2010-08-13 14:43:43\", \"Events\": 14996}, {\"finished\": \"2010-08-13 14:43:56\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:43:57\", \"Events\": 14299}, {\"finished\": \"2010-08-13 14:43:57\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:44:04\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:44:15\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:44:15\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:44:34\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:44:35\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:44:35\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:44:35\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:44:36\", \"Events\": 14995}, {\"finished\": \"2010-08-13 14:45:03\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:45:10\", \"Events\": 14998}, {\"finished\": \"2010-08-13 14:45:25\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:45:26\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:45:45\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:45:50\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:45:50\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:46:01\", \"Events\": 14997}, {\"finished\": \"2010-08-13 14:46:07\", \"Events\": 14996}, {\"finished\": \"2010-08-13 14:46:14\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:46:23\", \"Events\": 14999}, {\"finished\": \"2010-08-13 14:46:26\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:46:30\", \"Events\": 15000}, {\"finished\": \"2010-08-13 14:47:09\", \"Events\": 14999}, {\"finished\": \"2010-08-13 15:57:09\", \"Events\": 14996}, {\"finished\": \"2010-08-13 16:17:45\", \"Events\": 14997}], {\"taskmonid\": \"ganga:e60e5904-e63e-432f-b3df-63ca833cf080:\"}], \"lastfinished\": [[{\"finished\": \"2010-08-13 16:17:45\"}], {\"taskmonid\": \"ganga:e60e5904-e63e-432f-b3df-63ca833cf080:\"}], \"firststarted\": [[{\"started\": \"2010-08-13 13:51:21\"}], {\"taskmonid\": \"ganga:e60e5904-e63e-432f-b3df-63ca833cf080:\"}]}"

        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()

        jsonp_function = qsDict['jsonp_callback']
        result = "%s(%s);" % (jsonp_function, json)
        self.wfile.write(result)

        return

jobs_dictionary = {}
httpServerHost = 'localhost'
httpServerStartTryPort = 8080

# todo remove
#import os
#tempFilePath = os.path.join(config.Configuration.gangadir, 'process')
# saveProcessDetails()
# end remove


def start_server():

    t = HTTPServerThread("HTTP_monitoring")
    t.start()

if __name__ == '__main__':
    start_server()

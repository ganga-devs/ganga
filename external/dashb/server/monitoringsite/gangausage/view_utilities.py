import datetime
import time
import string
from operator import itemgetter
from models import GangaSession, GangaJobSubmitted

#time period per which the time charts are shown - could be day, week, month
global timePeriod 
#number days depending on the time period - for day - 1, for week - 7, for month 30 
global numberDays
#number of periods for the selected time range - this is the number of bars in the time charts
global numberPeriods
#selected from date of the time range of interest
global selectedFromDate


numberDays = 0
numberPeriods = 0
selectedFromDate = None
timePeriod = ''

#set the global variables
def setGlobals(number_days, number_periods, selected_from_date, time_period):

        global numberDays
        global numberPeriods
        global selectedFromDate
        global timePeriod 

        numberDays = number_days
        numberPeriods = number_periods
        selectedFromDate = selected_from_date
        timePeriod = time_period        

#start google chart utilities

#make stacked bar chart
def makeStackedBarChart(barchartData, barChartColors, max_y, maxCol):        
        
        from pygooglechart import Axis
        from pygooglechart import StackedVerticalBarChart

        #depending on the maxCol value set the max value of the Y axis so that the graphics is OK even for the biggest column
        maxY = max_y
        left_axis = range(0, maxY + 1, max_y/5)

        if maxCol < 50:
            maxY = 50
            left_axis = range(0, maxY + 1, 10)
        elif maxCol > 50 and maxCol <= 100:      
            maxY = 100
            left_axis = range(0, maxY + 1, 20)  
        elif maxCol > 100 and maxCol <= 200:
            maxY = 200
            left_axis = range(0, maxY + 1, 40)
        elif maxCol > 200 and maxCol <= 300:
            maxY = 300
            left_axis = range(0, maxY + 1, 60)
        elif maxCol > 300 and maxCol <= 400:                         
            maxY = 400
            left_axis = range(0, maxY + 1, 80)
        elif maxCol > 400 and maxCol <= 500:                         
            maxY = 500
            left_axis = range(0, maxY + 1, 100)
        elif maxCol > 500 and maxCol <= 600:                         
            maxY = 600
            left_axis = range(0, maxY + 1, 120)
        elif maxCol > 600 and maxCol <= 900:                         
            maxY = 900
            left_axis = range(0, maxY + 1, 180)
        elif maxCol > 900 and maxCol <= 3000:
            maxY = 3000
            left_axis = range(0, maxY + 1, 600)                
        elif maxCol > 3000 and maxCol <= 6000:
            maxY = 6000
            left_axis = range(0, maxY + 1, 1200)
        elif maxCol > 6000 and maxCol <= 9000:
            maxY = 9000
            left_axis = range(0, maxY + 1, 1800)
        elif maxCol > 9000 and maxCol <= 12000:                 
            maxY = 12000
            left_axis = range(0, maxY + 1, 2400)        
        elif maxCol > 12000 and maxCol <= 15000:                 
            maxY = 15000
            left_axis = range(0, maxY + 1, 3000)                
        elif maxCol > 15000 and maxCol <= 18000:
            maxY = 18000
            left_axis = range(0, maxY + 1, 3600) 
        elif maxCol > 18000 and maxCol <= 30000:
            maxY = 30000
            left_axis = range(0, maxY + 1, 6000) 
        elif maxCol > 30000 and maxCol <= 39000:
            maxY = 39000
            left_axis = range(0, maxY + 1, 7800) 
        elif maxCol > 39000 and maxCol <= 50000:
            maxY = 50000
            left_axis = range(0, maxY + 1, 10000)       
        elif maxCol > 50000 and maxCol <= 80000:
            maxY = 80000
            left_axis = range(0, maxY + 1, 16000)               
        elif maxCol > 80000 and maxCol <= 100000:
            maxY = 100000
            left_axis = range(0, maxY + 1, 20000) 
        elif maxCol > 100000 and maxCol <= 120000:
            maxY = 120000
            left_axis = range(0, maxY + 1, 24000) 
        elif maxCol > 120000 and maxCol <= 150000:
            maxY = 150000
            left_axis = range(0, maxY + 1, 30000) 
        elif maxCol > 150000 and maxCol <= 180000:
            maxY = 180000
            left_axis = range(0, maxY + 1, 36000) 
        elif maxCol > 180000 and maxCol <= 210000:
            maxY = 210000
            left_axis = range(0, maxY + 1, 42000) 
        else:
            maxY = 250000
            left_axis = range(0, maxY + 1, 50000) 

        barchart = None

        #depending on the number of periods set the width of the chart and width of the bars in the chart
        if numberPeriods < 20:
            barchart = StackedVerticalBarChart(50*numberPeriods, 150, y_range=(0, maxY))
            barchart.set_bar_width(23)        
        else:
            barchart = StackedVerticalBarChart(1000, 150, y_range=(0, maxY))        
            barchart.set_bar_width(20)        
                
        #set the data
        for data in barchartData:        
            barchart.add_data(data)

        #get the labes of the X axis - these are the time periods
        bottomAxisLabels = getBottomAxisLabels()

        #make 5 horizontal lines crossing the Y axis
        barchart.set_grid(0, 20)
        #set the colors of the time charts
        barchart.set_colours(barChartColors)
        
        #at (0,0) do not set a label, set the X and Y axis
        left_axis[0] = ''
        barchart.set_axis_labels(Axis.LEFT, left_axis)        
        barchart.set_axis_labels(Axis.BOTTOM, bottomAxisLabels)

        return (barchart, maxY, barChartColors)

#get X axis labels
def getBottomAxisLabels():        

        bottomAxisLabels = []

        for period in range(numberPeriods+1):

            #get the datetime of each period
            periodDate = selectedFromDate + datetime.timedelta(days=numberDays*period)

            #for month it is different - we get the first day of the month
            if timePeriod == 'month':
                periodDate = getMonthStartDate(selectedFromDate, period)

            #set labels for even periods, otherwise the labels are overlapping
            if(period % 2 == 0):        
                #different formatting for month and for day/week periods
                if timePeriod == 'month':
                    bottomAxisLabels.append(periodDate.strftime("%b %y"))
                else:
                    bottomAxisLabels.append(periodDate.strftime("%d %b"))       
            #for not even periods do not set label - space is required otherwise they are overlapping
            else:
                bottomAxisLabels.append('')

        return bottomAxisLabels

#make pie chart
def make_pie(d):
        
        #todo:this is hard coded -> there should be work around
        import sys
        mypath = '/home/ivan/django/externals/pygooglchart'
        if mypath not in sys.path:
            sys.path.append(mypath)

        from pygooglechart import PieChart3D
        # 600x170 is the optimal size of the pie chart I found
        chart = PieChart3D(600, 170)
        
        #sort the data by value of the dictionary
        sorted = d.items()
        sorted.sort(key = itemgetter(1), reverse=True)

        values = []
        pie_labels = []

        #fill the data and labels of the pie chart
        for i in range(len(sorted)):
            values.append(sorted[i][1])
            pie_labels.append('%s (%d)' % (sorted[i][0], sorted[i][1])) 
        
        #bind data and labels, set colors
        chart.add_data(values)
        chart.set_pie_labels(pie_labels)
        chart.set_colours(['F2510C', 'F2EE0C', '23F20C', '0CF2EE', '0C27F2', 'F20CEE'])

        return chart 

#get the max column value of the bar chart
def getMaxValueBarChart(barchart_data):
        
        cols = 0
        rows = len(barchart_data)
        if rows > 0:
            cols = len(barchart_data[0])
        maxCol = 0
        sumCol = 0        

        #get the max sum of a column -> this is the max column value 
        for j in range(cols):
            for i in range(rows):
                sumCol += barchart_data[i][j]
            if(sumCol > maxCol):
                maxCol = sumCol
            sumCol = 0  

        return maxCol

#end google chart utilities


#start datetime utilities

#get month first day - the first day of numberOfPeriod months after the date
def getMonthStartDate(date, numberOfPeriod):

        selectedFromDateMonth = date.month
        selectedFromDateYear = date.year

        targetStartDateMonth = selectedFromDateMonth + numberOfPeriod
        targetStartDateYear = selectedFromDateYear
                
        monthsAhead = selectedFromDateMonth + numberOfPeriod    

        if monthsAhead > 12:
            if monthsAhead % 12 == 0:
                targetStartDateMonth = 12
                targetStartDateYear += ((monthsAhead / 12) - 1) 
            else:               
                targetStartDateMonth = monthsAhead % 12
                targetStartDateYear += monthsAhead / 12 
            
        startDate = datetime.date(targetStartDateYear, targetStartDateMonth, 1)

        return startDate

#gets the start and end date of a period 
def getStartEndDateOfPeriod(numberOfPeriod):

        #get start and end date with adding days to the selected start date
        startDate = selectedFromDate + datetime.timedelta(days=numberDays*numberOfPeriod)
        endDate = selectedFromDate + datetime.timedelta(days=numberDays*(numberOfPeriod+1))

        #for month get the 1st day of the month for start and 1st day of the next month for end date
        if timePeriod == 'month':           
            startDate = getMonthStartDate(selectedFromDate, numberOfPeriod)
            
            targetEndDateYear = startDate.year
            targetEndDateMonth = startDate.month + 1

            if startDate.month == 12:
                targetEndDateYear += 1
                targetEndDateMonth = 1  

            endDate = datetime.date(targetEndDateYear, targetEndDateMonth, 1)

        #get seconds today
        now = datetime.datetime.now()                 
        secondsToday = now.hour*3600 + now.minute*60 + now.second
                        
        #get days diff from today to start and end date
        dateDiffStartDate = datetime.date.today() - startDate
        dateDiffEndDate = datetime.date.today() - endDate
        numberDaysToStart = dateDiffStartDate.days
        numberDaysToEnd = dateDiffEndDate.days
        
        startDateTime = time.time() - secondsToday - numberDaysToStart*24*3600
        endDateTime = time.time() - secondsToday - numberDaysToEnd*24*3600

        return (startDateTime, endDateTime)

def convertStringToDate(timestring):
        
    time_format = "%Y-%m-%d"
    selectedDatetime = datetime.datetime.fromtimestamp(time.mktime(time.strptime(timestring, time_format)))
    date = datetime.date(selectedDatetime.year, selectedDatetime.month, selectedDatetime.day)   

    return date
        
#end datetime utilities

#remove dictionary entries with small values -> leave only top 20
def reduceSmallParts(d):

        if len(d) < 20:
            return d

        #sort by values
        sorted = d.items()
        sorted.sort(key = itemgetter(1), reverse=True)

        itemList = []
        
        try:
            for i in range(20):
                itemList.append(sorted[i][0])
        except IndexError:
            pass

        resultDictionary = {}

        for item in itemList:
            resultDictionary[item] = d[item]

        return resultDictionary  


#increment dictionary value method
def increment(d,k):
        d.setdefault(k,0)
        d[k] += 1

#increment dictionary with given value
def incrementWithValue(d,k,value):
        d.setdefault(k,0)
        d[k] += value

#get experiment name
def getExperimentName(selectedExperiment):

        experimentName = 'Atlas'        

        if selectedExperiment is None or selectedExperiment == '-':
                experimentName = ''
        else:
                if selectedExperiment == 'lhc':
                        experimentName = 'LHCb'
                elif selectedExperiment == 'oth':        
                        experimentName = 'Other'

        return experimentName

#depending on the timePeriod selected get the number of days in this time period
def getNumberDays(timePeriod):

        numberDays = 1

        if timePeriod == 'week':
                numberDays = 7
        
        #for month we don't need number days - we get start and end date of the month 

        return numberDays

#gets time period by which time charts will be displayed
def getTimePeriod(periodDays):

        timePeriod = ''

        #if the difference between start and end date is less than 31 days -> show by day
        if periodDays <= 31:
                timePeriod = 'day'
        #if the difference between start and end date is between 32 and 299 days -> show by week
        elif periodDays < 300:
                timePeriod = 'week'
        #if the difference between start and end date is more than 299 days -> show by month
        else:
                timePeriod = 'month'    

        return timePeriod

#gets number of periods that will be shown in the time charts (number of days, number of weeks or number of months)
def getNumberPeriods(timePeriod, numberDays, selectedToDate, selectedFromDate):

        numPeriods = 0

        #if not month get the days difference between end and start date and devide by number of days(which is 1 for day period and 7 for week period)
        if timePeriod is not 'month':        
                dateDiff = selectedToDate - selectedFromDate
                numPeriods = dateDiff.days / numberDays                                         
        #if month get the months difference between the start and end date including the start and the end month
        else:
                fromYear = selectedFromDate.year
                fromMonth = selectedFromDate.month
                toYear = selectedToDate.year
                toMonth = selectedToDate.month

                if fromYear == toYear:
                        numPeriods = toMonth - fromMonth + 1
                elif toYear == fromYear + 1:
                        numPeriods = 12 - fromMonth + 1 + toMonth   
                else:
                        numPeriods = 12 - fromMonth + 1 + toMonth + 12*(toYear - fromYear - 1)                  

        return numPeriods

#fill dictionary methods
def incrementSessionVersions(s, dictionary):
        import re

        pattern = 'Ganga-\d-\d-\d'
        match = re.match(pattern, s.version)
                
        #not in the standart pattern for ganga versions
        if match is None:
            increment(dictionary, 'development versions')                    
        else:
            increment(dictionary, s.version) 

def incrementCernNonCernUsers(s, nonCernUsers, localUsers, cernUsers):
        if "cern.ch" not in s.host:
            dot_index = s.host.find('.')
            #other than cern.ch domain
            if dot_index > 0:                
                increment(nonCernUsers, s.user)
            #local domain
            else: 
                increment(localUsers, s.user)
        #cern.ch domain
        else:
            increment(cernUsers, s.user) 

def incrementNonCernInstallationCountry(s, non_cern_installations_country):
        if "cern.ch" not in s.host:
            #get the country extension of the host
            dot_index = s.host.rfind('.')
            if dot_index > 0:                
                increment(non_cern_installations_country, string.lower(s.host[dot_index+1:]))


def incrementNonCernInstallation(s, non_cern_installations, gangaInstallations):
        if "cern.ch" not in s.host:
            dot_index = s.host.find('.')
            #other than cern.ch hosts
            if dot_index > 0:                
                increment(non_cern_installations, string.lower(s.host[dot_index+1:]))
                increment(gangaInstallations, "Non CERN installations")
            #local domain
            else: 
                increment(gangaInstallations, "unknown")
        #cern installation
        else:
            increment(gangaInstallations, "CERN installations")
        
def incrementExperiments(s, experiments):                 
        if "Atlas" in s.runtime_packages:
            increment(experiments,"Atlas")        
        else:
            if "LHCb" in s.runtime_packages:
                increment(experiments,"LHCb")
            else:
                increment(experiments,'others')

def fillNonCernInstallationsUsers(sessions, reduced_non_cern_installations, non_cern_installations_users):
        #for each host get the host sessions
        for host in reduced_non_cern_installations.keys():
                hostSessions = []
                for s in sessions:
                        dotIndex = s.host.find('.')
                        if dotIndex > 0 and s.host[dotIndex+1:] == host:
                                hostSessions.append(s)  

                #get unique users for the host sessions
                uniqueUsers = {}
                for s in hostSessions: 
                        increment(uniqueUsers, s.user)
                non_cern_installations_users[host] = len(uniqueUsers)   

def fillNonCernInstallationsCountryUsers(sessions, reduced_non_cern_installations_country, non_cern_installations_country_users):
        #for each country get the country sessions
        for country in reduced_non_cern_installations_country.keys():
                countrySessions = []
                for s in sessions:
                        if 'cern.ch' not in s.host:
                                dotIndex = s.host.rfind('.')
                                if dotIndex > 0 and s.host[dotIndex+1:] == country:
                                        countrySessions.append(s)   
                #get unique users for the country sessions
                uniqueUsers = {}
                for s in countrySessions: 
                        increment(uniqueUsers, s.user)
                non_cern_installations_country_users[country] = len(uniqueUsers)

def fillUniqueUsersPerVersion(sessions, reducedVersions, versionsUsers):
        #for each version get the version sessions
        for version in reducedVersions.keys():
                versionSessions = []
                for session in sessions:
                        if session.version == version:
                                versionSessions.append(session)
        
                #get unique users for the version sessions
                versionUsers = {}
                for session in versionSessions:
                        increment(versionUsers, session.user) 
             
                versionsUsers[version] = len(versionUsers)

#get count of unique users for each experiment
def fillUniqueUsersPerExperiment(sessions, experimentsUsers, experimentName):

        atlasSessions = []
        lhcbSessions = []
        otherSessions = []
        
        #fill the atlas , lhcb and others sessions lists
        for session in sessions:
                if 'Atlas' in session.runtime_packages:
                        atlasSessions.append(session)
                elif 'LHCb' in session.runtime_packages:
                        lhcbSessions.append(session)
                else:
                        otherSessions.append(session)
    
        #get atlas unique users
        atlasUsers = {}
        for session in atlasSessions:
                increment(atlasUsers, session.user)

        #get lhcb unique users
        lhcbUsers = {}
        for session in lhcbSessions:
                increment(lhcbUsers, session.user)                                      
        
        #get others unique users
        otherUsers = {}
        for session in otherSessions:
                increment(otherUsers, session.user)                                     

        #depending on the selected experiment add dictionary entries either for a single experiment or for all
        if experimentName == '':
                if len(atlasUsers) > 0 : experimentsUsers['Atlas'] = len(atlasUsers)    
                if len(lhcbUsers) > 0 : experimentsUsers['LHCb'] = len(lhcbUsers)
                if len(otherUsers) > 0 : experimentsUsers['others'] = len(otherUsers)           
        elif experimentName == 'Atlas':
                if len(atlasUsers) > 0 : experimentsUsers['Atlas'] = len(atlasUsers)    
        elif experimentName == 'LHCb':      
                if len(lhcbUsers) > 0 : experimentsUsers['LHCb'] = len(lhcbUsers)       
        else:
                if len(otherUsers) > 0 : experimentsUsers['others'] = len(otherUsers) 

#get count of unique users for each session type    
def fillUniqueUsersPerSessionType(sessions, session_types, sessionTypesUsers):
        
        #for each session type get the session type sessions
        for sessionType in session_types.keys():
                sessionTypeSessions = []
                for session in sessions:
                        if session.session_type == sessionType:
                                sessionTypeSessions.append(session)
        
                #get unique users for the session type
                sessionTypeUsers = {}
                for session in sessionTypeSessions:
                        increment(sessionTypeUsers, session.user) 
             
                sessionTypesUsers[sessionType] = len(sessionTypeUsers)  

#fills ganga unique users 
def fillGangaUsers(gangaUsers, cernUsers, nonCernUsers, localUsers):

        if len(cernUsers) > 0:
                gangaUsers["CERN users"] = len(cernUsers)
        if len(nonCernUsers) > 0:
                gangaUsers["Non CERN users"] = len(nonCernUsers)
        if len(localUsers) > 0:
                gangaUsers["unknown users"] = len(localUsers)                                                   

#populate the dictionaries for session versions by period, sessions by period and domain and dictionary for unique users per period    
#each dictionary entry has a key number of period in the selected time range and represents a single bar in the time charts, the value of the dictionary entry is dictionary and it is represented in the different colors in the bar
def fillTimeChartDictionaries(sessions, experimentName, sessions_by_period_and_domain, session_versions_by_period, unique_users_per_period_and_experiment):
        
        for period in range(numberPeriods):
                #get start and end date of the period so that we can get the sessions for this period
                dateTimes = getStartEndDateOfPeriod(period)
                startDateTime, endDateTime = dateTimes[0], dateTimes[1]        

                #get the sessions for this period and experiment
                periodSessions = []
        
                if experimentName == '':
                        for s in sessions:
                                if s.time_start >= startDateTime and s.time_start <= endDateTime:
                                        periodSessions.append(s)            
                elif experimentName == 'Atlas' or experimentName == 'LHCb':
                        for s in sessions:
                                if s.time_start >= startDateTime and s.time_start <= endDateTime and experimentName in s.runtime_packages:
                                        periodSessions.append(s)            
                else:
                        for s in sessions:
                                if s.time_start >= startDateTime and s.time_start <= endDateTime and 'Atlas' not in s.runtime_packages and 'LHCb' not in s.runtime_packages:
                                        periodSessions.append(s)                    
        
                #dictionary with key domain and value number of sessions from this domain
                periodDomains = {}    
                #dictionary with key version and value number of sessions of this version
                periodSessionVersions = {}

                """
                periodNewOldVersions = {}
                periodNewOldVersions['version 5.5'] = 0
                periodNewOldVersions['other version'] = 0
                periodNewOldVersions['older version than 5.5'] = 0
                """

                for s in periodSessions:   
                        #fill period domains                     
                        dot_index = s.host.find('.')
                        if dot_index > 0:
                                import string                 
                                increment(periodDomains, string.lower(s.host[dot_index+1:]))                                 
                        else:
                                increment(periodDomains, 'unknown')                                               
            
                        #fill period new/old version
                        """
                        if s.version.find('Ganga-5-5-') > -1:
                                increment(periodNewOldVersions, 'version 5.5')
                        else:
                                import re

                                pattern = 'Ganga-\d-\d-\d'
                                match = re.match(pattern, s.version)
                
                                if match is None:
                                        increment(periodNewOldVersions, 'other version')                    
                                else:
                                        increment(periodNewOldVersions, 'older version than 5.5')                   
                        """

                        #fill period session versions
                        import re

                        pattern = 'Ganga-\d-\d-\d'
                        match = re.match(pattern, s.version)
                
                        if match is None:
                                increment(periodSessionVersions, 'development versions')                    
                        else:
                                increment(periodSessionVersions, s.version)                     

                #for each period create a dictionary entry with key number of period and value another dictionary with key host domain and value number of sessions from this host domain for this period
                sessions_by_period_and_domain[period+1] = periodDomains
                #for each period create a dictionary entry with key number of period and value another dictionary with key session version and value number of sessions of this version for this period
                session_versions_by_period[period+1] = periodSessionVersions

                #session_versions_by_period_and_domain[period+1] = periodNewOldVersions          

                #fill period unique users per experiment
                periodUniqueUsersPerExperiment = {}
                periodUniqueUsersPerExperiment["Atlas"] = 0
                periodUniqueUsersPerExperiment["LHCb"] = 0
                periodUniqueUsersPerExperiment["other"] = 0

                #if all experiments
                if experimentName == '':
                        
                        #fill experiment sessions       
                        atlasPeriodSessions = [s for s in periodSessions if 'Atlas' in s.runtime_packages]
                        lhcbPeriodSessions = [s for s in periodSessions if 'LHCb' in s.runtime_packages]
                        otherPeriodSessions = [s for s in periodSessions if 'LHCb' not in s.runtime_packages and 'Atlas' not in s.runtime_packages] 
        
                        atlasPeriodUniqueUsers = {}
                        lhcbPeriodUniqueUsers = {}
                        otherPeriodUniqueUsers = {} 
                
                        #iterate through each experiment sessions and increment unique users
                        for s in atlasPeriodSessions: increment(atlasPeriodUniqueUsers, s.user)
                        for s in lhcbPeriodSessions: increment(lhcbPeriodUniqueUsers, s.user)       
                        for s in otherPeriodSessions: increment(otherPeriodUniqueUsers, s.user)
                
                        #set dictionary values
                        periodUniqueUsersPerExperiment["Atlas"] = len(atlasPeriodUniqueUsers)
                        periodUniqueUsersPerExperiment["LHCb"] = len(lhcbPeriodUniqueUsers)
                        periodUniqueUsersPerExperiment["other"] = len(otherPeriodUniqueUsers)       
      
                #if atlas, lhcb or others set dictionary value only for it
                elif experimentName == 'Atlas':

                        atlasPeriodUniqueUsers = {}
                        for s in periodSessions: increment(atlasPeriodUniqueUsers, s.user)

                        periodUniqueUsersPerExperiment["Atlas"] = len(atlasPeriodUniqueUsers)
                elif experimentName == 'LHCb':

                        lhcbPeriodUniqueUsers = {}
                        for s in periodSessions: increment(lhcbPeriodUniqueUsers, s.user)
        
                        periodUniqueUsersPerExperiment["LHCb"] = len(lhcbPeriodUniqueUsers)
                else:
            
                        otherPeriodUniqueUsers = {}
                        for s in periodSessions: increment(otherPeriodUniqueUsers, s.user)

                        periodUniqueUsersPerExperiment["other"] = len(otherPeriodUniqueUsers)
        
                #for each period create a dictionary entry with key number of period and value another dictionary with key experiment and value unique users by this experiment
                unique_users_per_period_and_experiment[period+1] = periodUniqueUsersPerExperiment 

#prepares data source, colors and initial values for the google StackedBarChart used for ganga versions before and after 5.5 by time period
def prepareDataStackedBarChartSessionVersionsFixed(d):

        barchart_data = []
        versionList = ['version 5.5', 'older version than 5.5', 'other version']

        #for each version get list of unique users by period
        #example 3 periods, fist period 5, second period 2, third period 4 users -> [5,2,4]
        for version in versionList:

                versionPeriodUsers = []

                for period in range(numberPeriods):
                        innerDict = d[period+1]        
                        try:
                                versionPeriodUsers.append(innerDict[version])
                        except KeyError:
                                pass
                
                #append version period users list to the data source of the google chart
                barchart_data.append(versionPeriodUsers)        
        
        #get max value of the barchart data source columns so we can make chart with good dimensions
        maxCol = getMaxValueBarChart(barchart_data)

        #set colors of the time chart
        colorsSessionVersions = ['91A3B0', 'E30022', '0247FE']

        return makeStackedBarChart(barchart_data, colorsSessionVersions, 600, maxCol)

#prepares data source, colors and initial values for the google StackedBarChart used for job submission distrubution by time period
def prepareDataStackedBarChartJobSubmission(d):
       
        barchart_data = []
        jobsNumberList = ['non_split_jobs', 'master_jobs', 'sub_jobs']

        #for each jobNumber get list of jobNumbers
        #example 3 periods, fist period 5, second period 2, third period 4 users -> [5,2,4]
        for jobsNumber in jobsNumberList:

                jobsNumberPeriodUsers = []

                for period in range(numberPeriods):
                        innerDict = d[period+1]        
                        jobsNumberPeriodUsers.append(innerDict[jobsNumber])
                
                #append jobs number period list to the data source of the google chart
                barchart_data.append(jobsNumberPeriodUsers)        
        
        #get max value of the barchart data source columns so we can make chart with good dimensions
        maxCol = getMaxValueBarChart(barchart_data)

        #set colors of the time chart
        colors = ['00FAFA', '2560E8', '040C1F']    

        return makeStackedBarChart(barchart_data, colors, 100, maxCol)


#prepares data source, colors and initial values for the google StackedBarChart used for unique users per experiment and time period
def prepareDataStackedBarChartUsersByExperiment(d):
       
        barchart_data = []
        experimentList = ['Atlas', 'LHCb', 'other']

        #for each experiment get list of unique users by period
        #example 3 periods, fist period 5, second period 2, third period 4 users -> [5,2,4]
        for experiment in experimentList:

                experimentPeriodUsers = []

                for period in range(numberPeriods):
                        innerDict = d[period+1]        
                        experimentPeriodUsers.append(innerDict[experiment])
                
                #append experiment period users list to the data source of the google chart
                barchart_data.append(experimentPeriodUsers)        
        
        #get max value of the barchart data source columns so we can make chart with good dimensions
        maxCol = getMaxValueBarChart(barchart_data)

        #set colors of the time chart
        colorsUsersByExperiment = ['00FAFA', '2560E8', '040C1F']    

        return makeStackedBarChart(barchart_data, colorsUsersByExperiment, 100, maxCol)

#prepares data source, colors and initial values for the google StackedBarChart used for ganga sessions by version and time period
def prepareDataStackedBarChartSessionVersions(d, session_versions):   

        sorted = session_versions.items()
        sorted.sort(key = itemgetter(1), reverse=True)

        #d.key - period
        #d.value - dict: key-version, value-number of sessions of the version

        #get top 5 most used versions
        versionList = []
        
        try:
                for i in range(5):
                        versionList.append(sorted[i][0])
        except IndexError:
                pass
      
        barchart_data = []

        for version in versionList:
                #for each version get the list containing number of sessions of this version by period
                versionPeriodValues = []
                for period in range(numberPeriods):
                        innerDict = d[period+1]        
                        if version in innerDict:
                                versionPeriodValues.append(innerDict[version])
                        else:
                                versionPeriodValues.append(0)   
                #add the version list to the data source        
                barchart_data.append(versionPeriodValues) 

        #make another list for all the sessions that are not from the top 5 most used versions
        othersPeriodValues = []
        for period in range(numberPeriods):
                innerDict = d[period+1]
                sumOthers = sum(innerDict.values()) 
                for version in versionList:
                        if version in innerDict.keys():
                                sumOthers -= innerDict[version]
                othersPeriodValues.append(sumOthers)  
              
        sumOthers = sum(othersPeriodValues)
        if sumOthers > 0: 
                #if there are more than 5 used versions add others version that sumarizes them all except the top 5
                versionList.append('others')
                barchart_data.append(othersPeriodValues)

        #get max value of the barchart data source columns so we can make chart with good dimensions
        maxCol = getMaxValueBarChart(barchart_data)

        #set colors of the time chart
        colorsSessionVersions = ['07F7BF', '00FAFA', '24BFF2', '2560E8', '4F25E8', '040C1F']       

        chart, max_value, colors = makeStackedBarChart(barchart_data, colorsSessionVersions, 600, maxCol)

        return (chart, maxCol, colors, versionList)        

#prepares data source, colors and initial values for the google StackedBarChart used for ganga sessions by domain and time period
def prepareDataStackedBarChartSessionsByDomain(d, non_cern_installations):

        #get top 4 most used non cern domains, add cern.ch and others     
        sorted = non_cern_installations.items()
        sorted.sort(key = itemgetter(1), reverse=True)

        #d.key - period
        #d.value - dict: key-domain, value-number of sessions of the domain

        domainList = ['cern.ch']
        
        try:
                for i in range(4):
                        domainList.append(sorted[i][0])
        except IndexError:
                pass
      
        barchart_data = []

        for domain in domainList:
                #for each domain get the list containing number of sessions of this domain by period
                domainPeriodValues = []
                for period in range(numberPeriods):
                        innerDict = d[period+1]        
                        if domain in innerDict:
                                domainPeriodValues.append(innerDict[domain])
                        else:
                                domainPeriodValues.append(0) 
                #add the domain list to the data source           
                barchart_data.append(domainPeriodValues)        


        #make another list for all the sessions that are not from the cern.ch domain and from top 4 most used non cern domains
        othersPeriodValues = []
        for period in range(numberPeriods):
                innerDict = d[period+1]
                sumOthers = sum(innerDict.values())         
                for domain in domainList:
                        if domain in innerDict.keys():
                                sumOthers -= innerDict[domain]
                othersPeriodValues.append(sumOthers)        

        barchart_data.append(othersPeriodValues)

        domainList.append('others')
        
        #get max value of the barchart data source columns so we can make chart with good dimensions
        maxCol = getMaxValueBarChart(barchart_data)

        #set colors of the time chart
        colorsSessionsByDomain = ['07F7BF', '00FAFA', '24BFF2', '2560E8', '4F25E8', '040C1F']       

        chart, max_value, colors = makeStackedBarChart(barchart_data, colorsSessionsByDomain, 600, maxCol)

        return (chart, maxCol, colors, domainList)

#get sessions from database filtered by start date, end date and experiment
def getSessions(fromDate, toDate, experimentName):
        
        sessions = []

        if experimentName == '':
                sessions = GangaSession.objects.filter(time_start__gte=fromDate, time_start__lte=toDate)
        elif experimentName == 'Atlas' or experimentName == 'LHCb':
                sessions = GangaSession.objects.filter(time_start__gte=fromDate, time_start__lte=toDate, runtime_packages__icontains=experimentName)
        else:
                sessions = GangaSession.objects.filter(time_start__gte=fromDate, time_start__lte=toDate).exclude(runtime_packages__icontains='Atlas').exclude(runtime_packages__icontains='LHCb')

        return sessions

#get jos submissions from database, filtered by start and end date
def getJobSubmissions(fromTime, toTime, experimentName):

        fromDate = datetime.datetime.fromtimestamp(fromTime).date()
        toDate = datetime.datetime.fromtimestamp(toTime).date()

        jobSubmissions = []

        if experimentName == '':
                jobSubmissions = GangaJobSubmitted.objects.filter(date__gte=fromDate, date__lt=toDate)
        elif experimentName == 'Atlas' or experimentName == 'LHCb':
                jobSubmissions = GangaJobSubmitted.objects.filter(date__gte=fromDate, date__lt=toDate, runtime_packages__icontains=experimentName)
        else: 
                jobSubmissions = GangaJobSubmitted.objects.filter(date__gte=fromDate, date__lt=toDate).exclude(runtime_packages__icontains='Atlas').exclude(runtime_packages__icontains='LHCb')

        return jobSubmissions

def fillJobSubmissionDictionary(job_submissions, job_submission_distribution):

        for period in range(numberPeriods):
                #get start and end date of the period so that we can get the job submissions for this period
                dateTimes = getStartEndDateOfPeriod(period)
                startDateTime, endDateTime = dateTimes[0], dateTimes[1]        

                startDate = datetime.datetime.fromtimestamp(startDateTime).date()
                endDate = datetime.datetime.fromtimestamp(endDateTime).date()

                #get the job submissions for this period
                periodJobSubmissions = []

                for jsd in job_submissions:
                        if jsd.date >= startDate and jsd.date < endDate:
                                periodJobSubmissions.append(jsd)

                period_job_submission_distribution = {}
                period_job_submission_distribution['non_split_jobs'] = 0
                period_job_submission_distribution['master_jobs'] = 0
                period_job_submission_distribution['sub_jobs'] = 0

                for jobSubmission in periodJobSubmissions:
                        period_job_submission_distribution['non_split_jobs'] += jobSubmission.plain_jobs
                        period_job_submission_distribution['master_jobs'] += jobSubmission.master_jobs
                        period_job_submission_distribution['sub_jobs'] += jobSubmission.sub_jobs

                job_submission_distribution[period+1] = period_job_submission_distribution

#fills job submission dictionaries by site and country and also cern/non cern 
def fillJobSubmissionBySiteAndCountry(job_submissions, job_submission_by_non_cern_site, job_submission_by_non_cern_country, job_submission_cern_non_cern, job_submission_distribution, job_submission_by_application, job_submission_by_backend):

        plain_jobs_counter = 0
        master_jobs_counter = 0
        sub_jobs_counter = 0

        for js in job_submissions:

                plain_jobs_counter += js.plain_jobs
                master_jobs_counter += js.master_jobs 
                sub_jobs_counter += js.sub_jobs 

                if 'cern.ch' not in js.host:

                        #increment Non CERN jobs count
                        incrementWithValue(job_submission_cern_non_cern, "Non CERN submitted jobs", js.plain_jobs)
                        incrementWithValue(job_submission_cern_non_cern, "Non CERN submitted jobs", js.sub_jobs)
                        
                        #get site from host
                        dot_index_left = js.host.find('.')      
                        if dot_index_left > 0:
                                current_site = string.lower(js.host[dot_index_left+1:])

                                #increment jobs count by site
                                incrementWithValue(job_submission_by_non_cern_site, current_site, js.plain_jobs)
                                incrementWithValue(job_submission_by_non_cern_site, current_site, js.sub_jobs)
        
                        #get country from host
                        dot_index_right = js.host.rfind('.')    
                        if dot_index_right > 0:
                                current_country = string.lower(js.host[dot_index_right+1:])

                                #increment jobs count by country
                                incrementWithValue(job_submission_by_non_cern_country, current_country, js.plain_jobs)
                                incrementWithValue(job_submission_by_non_cern_country, current_country, js.sub_jobs)
                else:

                        #increment CERN jobs count
                        incrementWithValue(job_submission_cern_non_cern, "CERN submitted jobs", js.plain_jobs)
                        incrementWithValue(job_submission_cern_non_cern, "CERN submitted jobs", js.sub_jobs)

                #increment jobs submitted by application
                incrementWithValue(job_submission_by_application, js.application, js.plain_jobs)
                incrementWithValue(job_submission_by_application, js.application, js.sub_jobs)

                #increment jobs submitted by backend
                incrementWithValue(job_submission_by_backend, js.backend, js.plain_jobs)
                incrementWithValue(job_submission_by_backend, js.backend, js.sub_jobs)


        if plain_jobs_counter > 0: job_submission_distribution['non_split_jobs'] = plain_jobs_counter
        if master_jobs_counter > 0: job_submission_distribution['master_jobs'] = master_jobs_counter
        if sub_jobs_counter > 0: job_submission_distribution['sub_jobs'] = sub_jobs_counter
                        


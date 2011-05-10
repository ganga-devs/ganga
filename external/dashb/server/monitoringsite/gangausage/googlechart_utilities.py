import datetime
import time
import string
from operator import itemgetter

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
        elif maxCol > 600 and maxCol <= 3000:
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
        else:
            maxY = 100000
            left_axis = range(0, maxY + 1, 20000) 

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
def setNumberDays(timePeriod):

        numberDays = 1

        if timePeriod == 'week':
                numberDays = 7
        
        #for month we don't need number days - we get start and end date of the month 

        return numberDays

#gets number of periods that will be shown in the time charts (number of days, number of weeks or number of months)
def setNumberPeriods(timePeriod, numberDays, selectedToDate, selectedFromDate):

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


#populate the dictionary for sessions by period and domain and dictionary for unique users per period    
def fillTimeChartDictionaries(sessions, experimentName, sessions_by_period_and_domain, session_versions_by_period, unique_users_per_period_and_experiment):
        
        for period in range(numberPeriods):
                dateTimes = getStartEndDateOfPeriod(period)
                startDateTime, endDateTime = dateTimes[0], dateTimes[1]        

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
        
                periodDomains = {}    
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

                sessions_by_period_and_domain[period+1] = periodDomains
                session_versions_by_period[period+1] = periodSessionVersions
                #session_versions_by_period_and_domain[period+1] = periodNewOldVersions          

                periodUniqueUsersPerExperiment = {}
                periodUniqueUsersPerExperiment["Atlas"] = 0
                periodUniqueUsersPerExperiment["LHCb"] = 0
                periodUniqueUsersPerExperiment["other"] = 0

                if experimentName == '':
                        
                        atlasPeriodSessions = [s for s in periodSessions if 'Atlas' in s.runtime_packages]
                        lhcbPeriodSessions = [s for s in periodSessions if 'LHCb' in s.runtime_packages]
                        otherPeriodSessions = [s for s in periodSessions if 'LHCb' not in s.runtime_packages and 'Atlas' not in s.runtime_packages] 
        
                        atlasPeriodUniqueUsers = {}
                        lhcbPeriodUniqueUsers = {}
                        otherPeriodUniqueUsers = {} 
           
                        for s in atlasPeriodSessions: increment(atlasPeriodUniqueUsers, s.user)
                        for s in lhcbPeriodSessions: increment(lhcbPeriodUniqueUsers, s.user)       
                        for s in otherPeriodSessions: increment(otherPeriodUniqueUsers, s.user)
                
                        periodUniqueUsersPerExperiment["Atlas"] = len(atlasPeriodUniqueUsers)
                        periodUniqueUsersPerExperiment["LHCb"] = len(lhcbPeriodUniqueUsers)
                        periodUniqueUsersPerExperiment["other"] = len(otherPeriodUniqueUsers)       
      
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
        
                unique_users_per_period_and_experiment[period+1] = periodUniqueUsersPerExperiment 


def prepareDataStackedBarChartSessionVersionsFixed(d):

        barchart_data = []
        versionList = ['version 5.5', 'older version than 5.5', 'other version']

        for version in versionList:

                versionPeriodUsers = []

                for period in range(numberPeriods):
                        innerDict = d[period+1]        
                        try:
                                versionPeriodUsers.append(innerDict[version])
                        except KeyError:
                                pass
                
                barchart_data.append(versionPeriodUsers)        
        
        maxCol = getMaxValueBarChart(barchart_data)

        colorsSessionVersions = ['91A3B0', 'E30022', '0247FE']

        return makeStackedBarChart(barchart_data, colorsSessionVersions, 600, maxCol)

def prepareDataStackedBarChartUsersByExperiment(d):
       
        barchart_data = []
        experimentList = ['Atlas', 'LHCb', 'other']

        for experiment in experimentList:

                experimentPeriodUsers = []

                for period in range(numberPeriods):
                        innerDict = d[period+1]        
                        experimentPeriodUsers.append(innerDict[experiment])
                
                barchart_data.append(experimentPeriodUsers)        
        
        maxCol = getMaxValueBarChart(barchart_data)

        colorsUsersByExperiment = ['00FAFA', '2560E8', '040C1F']    

        return makeStackedBarChart(barchart_data, colorsUsersByExperiment, 100, maxCol)


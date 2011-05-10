from django.http import HttpResponse
from django.shortcuts import render_to_response
from django.views.decorators.cache import cache_page
from django.template import RequestContext 

from search import SearchForm
from view_utilities import *
from operator import itemgetter

def current(request):
    #check for post data
    if request.method == 'POST':

        form = SearchForm(request.POST)
        selectedExperiment = form.data['experiment']

        if form.is_valid():

            selectedFromDate = form.cleaned_data['from_date']
            selectedToDate = form.cleaned_data['to_date']
            return return_response(request, 0, selectedFromDate=selectedFromDate, selectedToDate=selectedToDate, selectedExperiment=selectedExperiment)          

        else:
            #return default period        
            return return_response(request, 21, selectedExperiment=selectedExperiment)
    elif request.method == 'GET':

        #import datetime

        selectedExperiment = None
        exp = request.GET.get('e')
        if exp is not '-':
            selectedExperiment = exp
        
        f_d = request.GET.get('from_date')
        t_d = request.GET.get('to_date')

        if f_d is not None and str(f_d) is not "":

            fromDate = convertStringToDate(f_d)

            if t_d is not None and str(t_d) is not "":
                toDate = convertStringToDate(t_d)       
                return return_response(request, 0, selectedFromDate=fromDate, selectedToDate=toDate, selectedExperiment=selectedExperiment)                  
            else:
                return return_response(request, 0, selectedFromDate=fromDate, selectedExperiment=selectedExperiment)
        
        return return_response(request, 21, selectedExperiment=selectedExperiment)

    #return default period
    return return_response(request, 21)

#uncomment caching when in production
#@cache_page(10 * 60)
def dayView(request):
    return return_response(request, 1)

#@cache_page(10 * 60)
def weekView(request):
    return return_response(request, 7)

#@cache_page(10 * 60)
def monthView(request):        
    return return_response(request, 30) 

def return_response(request, numberDays, selectedFromDate=None, selectedToDate=None, selectedExperiment=None):

    import time
    import datetime
    import string       

    now = datetime.datetime.now()                 
    seconds_today = now.hour*3600 + now.minute*60 + now.second

    display_error_from_date = 'none'
    display_error_to_date_less_than_from_date = 'none'                

    if selectedFromDate is not None:                        
        
        dateDiff = datetime.date.today() - selectedFromDate

        numberDays = dateDiff.days
        if(numberDays < 0):
            display_error_from_date = 'block'
        elif selectedToDate is not None and selectedToDate < selectedFromDate:
            display_error_to_date_less_than_from_date = 'block'                 
    else:        
        selectedFromDate = datetime.date.today() - datetime.timedelta(days=numberDays)        
        
    last_ndays = time.time() - seconds_today - numberDays*24*3600

    experimentName = getExperimentName(selectedExperiment)      

    toDate = time.time() - seconds_today + 24*3600# we want till the end of the current date

    if selectedToDate is None:                
        selectedToDate = datetime.date.today() + datetime.timedelta(days=1)
    else:
        selectedToDate = selectedToDate + datetime.timedelta(days=1)
        toDateDiff = datetime.date.today() - selectedToDate
        toNumberDays = toDateDiff.days        
        toDate = time.time() - seconds_today - toNumberDays*24*3600

    nsessions = 0               
    sessions = getSessions(last_ndays, toDate, experimentName)
    job_submissions = getJobSubmissions(last_ndays, toDate, experimentName)  
        
    versions = {}
    session_types = {}
    experiments = {}
    non_cern_installations = {}
    non_cern_installations_country = {} 
    non_cern_installations_users = {}   
    non_cern_installations_country_users = {}       
    sessions_by_period_and_domain = {}
    #session_versions_by_period_and_domain = {}
    session_versions = {}          
    session_versions_by_period = {}     
    unique_users_per_period_and_experiment = {} 
    cernUsers = {}
    nonCernUsers = {}
    localUsers = {}
    users = {}
    user_versions = {}  
    gangaInstallations = {}  
    versionsUsers = {}   
    experimentsUsers = {}
    sessionTypesUsers = {} 
    gangaUsers = {} 
    job_submission_distribution_by_period = {}        
    job_submission_distribution = {}
    job_submission_by_non_cern_site = {}
    job_submission_by_non_cern_country = {}
    job_submission_cern_non_cern = {}
    job_submission_by_application = {}
    job_submission_by_backend = {}                              
     
    dateDiff = selectedToDate - selectedFromDate        
    periodDays = dateDiff.days

    timePeriod = getTimePeriod(periodDays)                               

    nsessions = len(sessions)       
                                                                
    #populate the dictionaries for ganga usage statistics
    for s in sessions:

        increment(versions,s.version)
        increment(session_types,s.session_type)
        increment(users,s.user)
        incrementSessionVersions(s, session_versions)  
        incrementNonCernInstallation(s, non_cern_installations, gangaInstallations)
        incrementNonCernInstallationCountry(s, non_cern_installations_country)
        incrementCernNonCernUsers(s, nonCernUsers, localUsers, cernUsers)
        incrementExperiments(s, experiments)

    reduced_non_cern_installations = reduceSmallParts(non_cern_installations)
    reduced_non_cern_installations_country = reduceSmallParts(non_cern_installations_country)
    reducedVersions = reduceSmallParts(versions)        

    fillNonCernInstallationsUsers(sessions, reduced_non_cern_installations, non_cern_installations_users)       
    fillNonCernInstallationsCountryUsers(sessions, reduced_non_cern_installations_country, non_cern_installations_country_users)                
        
    fillUniqueUsersPerVersion(sessions, reducedVersions, versionsUsers) 
    fillUniqueUsersPerExperiment(sessions, experimentsUsers, experimentName)
    fillUniqueUsersPerSessionType(sessions, session_types, sessionTypesUsers)   

    numberDays = getNumberDays(timePeriod)      
    numPeriods = getNumberPeriods(timePeriod, numberDays, selectedToDate, selectedFromDate)     
                
    #sets variables used in other modules
    setGlobals(numberDays, numPeriods, selectedFromDate, timePeriod)         

    fillTimeChartDictionaries(sessions, experimentName, sessions_by_period_and_domain, session_versions_by_period, unique_users_per_period_and_experiment)

    fillJobSubmissionDictionary(job_submissions, job_submission_distribution_by_period) 
    fillJobSubmissionBySiteAndCountry(job_submissions, job_submission_by_non_cern_site, job_submission_by_non_cern_country, job_submission_cern_non_cern, job_submission_distribution, job_submission_by_application, job_submission_by_backend)

    #set experiment name for displaying in the page
    if experimentName == '':
        experimentName = 'All'  

    fillGangaUsers(gangaUsers, cernUsers, nonCernUsers, localUsers)

    #non cern installation sites plus one for cern.ch if any cern installations  
    numberSites = len(non_cern_installations)  
    if gangaInstallations.has_key("CERN installations") and gangaInstallations["CERN installations"] > 0: 
        numberSites += 1

    chart1 = make_pie(reduceSmallParts(versions))
    chart2 = make_pie(session_types)
    chart3 = make_pie(experiments)
             
    chart4 = make_pie(reduced_non_cern_installations)

    chart6, max_value_sessions, colorsSessionsByDomain, domainList  = prepareDataStackedBarChartSessionsByDomain(sessions_by_period_and_domain, non_cern_installations) 
    chart7 = make_pie(reduced_non_cern_installations_country)    
    chart8 = make_pie(gangaInstallations) 

    chart11 = make_pie(reduceSmallParts(versionsUsers))   
    chart12 = make_pie(sessionTypesUsers)
    chart13 = make_pie(experimentsUsers)

    chart14, max_value_users, colorsUsersByExperiment = prepareDataStackedBarChartUsersByExperiment(unique_users_per_period_and_experiment)
        
    chart15 = make_pie(non_cern_installations_users)
    chart16 = make_pie(non_cern_installations_country_users)
    chart17 = make_pie(gangaUsers)      

    #chart18, max_value_session_version_fixed, colorsSessionVersionsFixed = prepareDataStackedBarChartSessionVersionsFixed(session_versions_by_period_and_domain)

    chart19, max_value_session_version, colorsSessionVersions, versionList = prepareDataStackedBarChartSessionVersions(session_versions_by_period, session_versions)

    chart20, max_value_submitted_jobs, colorsSubmittedJobs = prepareDataStackedBarChartJobSubmission(job_submission_distribution_by_period)

    chart21 = make_pie(job_submission_distribution)
    chart22 = make_pie(reduceSmallParts(job_submission_by_non_cern_site))
    chart23 = make_pie(reduceSmallParts(job_submission_by_non_cern_country))
    chart24 = make_pie(reduceSmallParts(job_submission_cern_non_cern))                                  
    chart25 = make_pie(reduceSmallParts(job_submission_by_application))                                 
    chart26 = make_pie(reduceSmallParts(job_submission_by_backend))                                     

    form = SearchForm()         

    startdate = selectedFromDate

    selectedToDate = selectedToDate - datetime.timedelta(days=1)        

    enddate = selectedToDate        
    if enddate is None:                
        enddate = datetime.date.today()

    return render_to_response('gangausage/index.html', 
                              {'sessions': sessions, 
                               'non_cern_installations': non_cern_installations,
                               'sessions_by_period_and_domain' : len(sessions_by_period_and_domain),                 
                               'version_piechart_url':chart1.get_url(), 
                               'session_type_piechart_url':chart2.get_url(),
                               'experiments_piechart_url':chart3.get_url(),
                               'non_cern_installations_url':chart4.get_url(),
                               'sessions_period_and_domain' : chart6.get_url(),
                               'max_value_sessions' : max_value_sessions, 
                               'non_cern_installations_country_url':chart7.get_url(),
                               'ganga_installations_percentage_url' : chart8.get_url(), 
                               'versions_users_url' : chart11.get_url(),
                               'session_types_users_url' : chart12.get_url(), 
                               'experiment_users_url' : chart13.get_url(), 
                               'users_per_period_and_experiment_url' : chart14.get_url(),
                               'non_cern_installations_users_url' : chart15.get_url(),
                               'non_cern_installations_country_users_url' : chart16.get_url(),
                               'ganga_installations_users_percentage_url' : chart17.get_url(),
                               #'session_versions_period_fixed' : chart18.get_url(),
                               'session_versions_period' : chart19.get_url(),
                               'job_submission_distribution_by_period_url' : chart20.get_url(),
                               'job_submission_distribution_url' : chart21.get_url(),
                               'job_submission_by_site_url' : chart22.get_url(),
                               'job_submission_by_country_url' : chart23.get_url(),
                               'job_submission_cern_non_cern_url' : chart24.get_url(),  
                               'job_submission_by_application_url' : chart25.get_url(), 
                               'job_submission_by_backend_url' : chart26.get_url(),     
                               'job_submission_distribution' : job_submission_distribution,
                               'colorsSubmittedJobs' : colorsSubmittedJobs,
                               'max_value_submitted_jobs' : max_value_submitted_jobs,
                               'versionList' : versionList,
                               'max_value_users' : max_value_users,     
                               'versionsUsersCount' : len(versionsUsers),
                               'sessionTypesUsersCount' : len(sessionTypesUsers), 
                               'experimentUsersCount' : len(experimentsUsers),  
                               'nusers' : len(users),
                               'nsessions' : nsessions,
                               'form': form,
                               'display_error_from_date': display_error_from_date,
                               'display_error_to_date_less_than_from_date': display_error_to_date_less_than_from_date,
                               'domains' : domainList,
                               'startdate': startdate,
                               'enddate' : enddate,
                               'numberSites' : numberSites,
                               'experimentName' : experimentName,
                               'timePeriod' : timePeriod,
                               'colorsSessionsByDomain' : colorsSessionsByDomain, 
                               'colorsUsersByExperiment' : colorsUsersByExperiment,
                               #'colorsSessionVersionsFixed' : colorsSessionVersionsFixed,
                                'max_value_session_version' : max_value_session_version,
                               'colorsSessionVersions' : colorsSessionVersions,
                               'ndays':numberDays},
                                context_instance = RequestContext(request))

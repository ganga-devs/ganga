// This file is part of the jJobMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 28.05.2010 Created
//

function Settings() {

    // Application specific settings - START
    this.Application = {
        'userSelection': false, // Display user selection page? (true|false)
        'jsonp': true, // allow requests to other hosts
	'pageTitle': 'Job Monitoring', // Page title
        'footerTxt': 'jJobMonitoring &copy;2010', // Footer text
        'supportLnk': 'https://twiki.cern.ch/twiki/bin/view/ArdaGrid/TaskMonitoringWebUI', // Link to support page
        'logoLnk': 'media/images/ganga_logo_72dpi.png', // Link to page logo
        'usersListLbl': 'Users List',
        'mainsLbl': 'Jobs',
        'subsLbL': 'Subjobs',
        'modelDefaults': { // Here You can set up model (data.js) default values
            'user': '',
            'from': 0,
            'till': 0,
            'timeRange': 'lastDay',
            'refresh': 0,
            'tid': '',
            'p': 1,
            'sorting': [],
            'or': [], // opened table rows
            'uparam': [] // user defined params (for params that cannot be shared between use cases)
        }
    };
    // Application specific settings - FINISH

    // Users list settings - START
    this.Users = {
        'dataURL': 'http://localhost/?list=users',
        'searchLabel': 'Search for user ',
	'dataURL_params': function(Data) {
            return {};
        },
        'translateData': function(dataJSON) {
            var usersList = Array();
            var dataArr = dataJSON.basicData[0];
            for (i in dataArr) {
                usersList.push(dataArr[i].GridName);//.replace(/"/g, ''));
            }
            return usersList;
        }
    };
    // Users list settings - FINISH
    
    // User Tasks settings - START
    this.Mains = {
        'dataURL': 'http://localhost/?list=jobs',
        'dataURL_params': function(Data) {
            obj = {
                'usergridname':Data.user,
                'from':Data.ts2iso(Data.from,2),
                'to':Data.ts2iso(Data.till,3),
                'timerange':Data.timeRange,
                'typeofrequest':'A'
            };
            return obj;
        },
        'expandableRows':true,
        'multipleER':false,
        'expandData': {

		'dataFunction': function(rowDataSet, jsonDataSet) {
                var properties = false;
                var html = false;
                var table = false;
                
                properties = [
                ['inputdir', rowDataSet.inputdir],
                ['outputdir', rowDataSet.outputdir],
                ['uuid', rowDataSet.uuid]
                ];

                return {'properties':properties,'table':table,'html':html};
		}
        },
        'sorting':[1,'desc'],
        'iDisplayLength': 25,
        'tblLabels': ['id','status','name', 'application','backend','subjobs','submitted','running','completed','failed', 'Graphicaly','link','actualCE'],
        'aoColumns': [
            {"sWidth":"80px", "sType": "integer-in-tag"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sClass":"numericTD","sWidth":"70px", "sType": "integer-in-tag"},
            {"sClass":"numericTD","sWidth":"70px", "sType": "integer-in-tag"},
            {"sClass":"numericTD","sWidth":"70px", "sType": "integer-in-tag"},
            {"sClass":"numericTD","sWidth":"70px", "sType": "integer-in-tag"},
            {"sClass":"numericTD","sWidth":"70px", "sType": "integer-in-tag"},  
            {"sClass":"graphicaly","sWidth":"200px"}, 
            {"sClass":"link","sWidth":"200px"},    
            null
        ],
        'getDataArray': function(data) {
            return data.user_taskstable;
        },
        /*'translateData': function(dataJSON) {
            var tasksArr = Array();
            for (i in dataJSON) {
                tasksArr.push(Array(
                    ('<div class="wrapColumn" title="'+dataJSON[i].TASKMONID+'"><a class="tmIdClick">'+dataJSON[i].TASKMONID+'</a></div>' || 'default'),
                    ('<a class="noJobsClick">'+dataJSON[i].NUMOFJOBS+'</a>' || '0'),
                    ('<a class="noPendClick">'+dataJSON[i].PENDING+'</a>' || '0'),
                    ('<a class="noRunnClick">'+dataJSON[i].RUNNING+'</a>' || '0'),
                    ('<a class="noSuccClick">'+dataJSON[i].SUCCESS+'</a>' || '0'),
                    ('<a class="noFailClick">'+dataJSON[i].FAILED+'</a>' || '0'),
                    ('<a class="noUnknClick">'+dataJSON[i].UNKNOWN+'</a>' || '0'),
                    'default'
                ));
            }
            return tasksArr;
        },*/
        'translateData': function(dataJSON) {

            var jobStatuses = {
                'new':'New',
                'submitting':'Submitting',
                'submitted':'Submitted',
                'running':'Running',
                'completed':'Completed',
                'killed':'Killed',
                'unknown':'Unknown',
                'incomplete':'Incomplete',
                'failed':'Failed'
            };

            var tasksArr = Array();
            for (i in dataJSON) {

                if(dataJSON[i].subjobs != '0')
                {
                tasksArr.push(Array(
                    ('<a>' + dataJSON[i].id + '</a>'),   
                    (jobStatuses[dataJSON[i].status] ? '<div class="status '+jobStatuses[dataJSON[i].status]+'">'+dataJSON[i].status+'</div>' : 'Unknown'),     
                    (dataJSON[i].name || ''),
                    (dataJSON[i].application || ''),
                    (dataJSON[i].backend || ''),
                    ('<a class="noJobsClick">'+dataJSON[i].subjobs+'</a>' || '0'),
                    ('<a class="">'+dataJSON[i].submitted+'</a>' || '0'),
                    ('<a class="noRunnClick">'+dataJSON[i].running+'</a>' || '0'),
                    ('<a class="">'+dataJSON[i].completed+'</a>' || '0'),
                    ('<a class="noFailClick">'+dataJSON[i].failed+'</a>' || '0'),
                    ('<img src="http://chart.apis.google.com/chart?chbh=a,0&chs=130x15&cht=bhs&chco=59D118,C50000,3072F3,FF9900&chds=0,'+dataJSON[i].subjobs+',0,'+dataJSON[i].subjobs+',0,'+dataJSON[i].subjobs+',0,'+dataJSON[i].subjobs+',0,'+dataJSON[i].subjobs+'&chd=t:'+dataJSON[i].completed+'|'+dataJSON[i].failed+'|'+dataJSON[i].running+'|'+dataJSON[i].submitted+'" />'),
                    ('<div style="width:50px;">'+dataJSON[i].link+'</div>'),
                    (dataJSON[i].actualCE || '')
                ));
                }
                else
                {
                tasksArr.push(Array(
                    (dataJSON[i].id),   
                    (jobStatuses[dataJSON[i].status] ? '<div class="status '+jobStatuses[dataJSON[i].status]+'">'+dataJSON[i].status+'</div>' : 'Unknown'),     
                    (dataJSON[i].name || ''),
                    (dataJSON[i].application || ''),
                    (dataJSON[i].backend || ''),
                    (dataJSON[i].subjobs || '0'),
                    (dataJSON[i].submitted || '0'),
                    (dataJSON[i].running || '0'),
                    (dataJSON[i].completed || '0'),
                    (dataJSON[i].failed || '0'),
                    ('<div style="width:125px;">&nbsp;</div>'),       
                    ('<div style="width:50px;">'+dataJSON[i].link+'</div>'),
                    (dataJSON[i].actualCE || '')
                ));     
                }
            }
            return tasksArr;
        },

        'setupUserParams': function(Data, el, aPos) {
            var classTranslate = {
                'tmIdClick':'all',
                'noJobsClick':'all',
                'noPendClick':'P',
                'noRunnClick':'R',
                'noSuccClick':'S',
                'noFailClick':'F',
                'noUnknClick':'U'
            };
            Data.uparam = [classTranslate[$(el).find('a').attr('class')]];
            Data.tid = Data.mem.mains.data[aPos[0]].id; 
        },
        'charts': [
            {
                'ajax':true,
                'dataURL': 'http://localhost/?list=jobs_statuses',
                'dataURL_params': function(Data) { 
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj; 
                },
                // translates data onto requires format:
                // {"chd":"t:60,40","chl":"Hello|World"}
                'translateData':function(dataJSON) {    
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Jobs status',
                    'cht':'p3',
                    'chs':'350x130'
                }
            },
            {
                'ajax':true,
                'dataURL': 'http://localhost/?list=jobs_backends',
                'dataURL_params': function(Data) { 
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj; 
                },
                'translateData':function(dataJSON) {    
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Jobs backend',
                    'cht':'p3',
                    'chs':'350x130'
                }
            },
            {
                'ajax':true,
                'dataURL': 'http://localhost/?list=jobs_applications',
                'dataURL_params': function(Data) { 
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj;                 
                },
                'translateData':function(dataJSON) {    
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Jobs application',
                    'cht':'p3',
                    'chs':'350x130'
                }
            }   
        ]
    };
    // User Tasks settings - FINISH
    
    // Task Jobs settings - START
    this.Subs = {
        'dataURL': 'http://localhost/?list=subjobs',
        'dataURL_params': function(Data) {
            obj = {
                'taskmonid':Data.tid,
                'from':Data.ts2iso(Data.from,2),
                'to':Data.ts2iso(Data.till,3),
                'timerange':Data.timeRange,
                'what':(Data.uparam[0] || 'all')
            };
            return obj;
        },
        'expandableRows':false, 
        'sorting':[1,'desc'],
        'iDisplayLength': 25,
        'tblLabels': ['fqid','status','name','application','backend' ,'actualCE'],
        'aoColumns': [
            {"sWidth":"90px", "sType": "numeric-float" },
            {"sWidth":"90px"},
            {"sWidth":"100px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sWidth":"70px"}
        ],
        'getDataArray': function(data) {
            return data.taskjobs;
        },
        'translateData': function(dataJSON) {
            var jobStatuses = {
                'new':'NewSubjob',
                'submitting':'SubmittingSubjob',
                'submitted':'SubmittedSubjob',
                'running':'RunningSubjob',
                'completed':'CompletedSubjob',
                'killed':'KilledSubjob',
                'unknown':'UnknownSubjob',
                'incomplete':'IncompleteSubjob',
                'failed':'FailedSubjob'
            };
            var tasksArr = Array();
            for (i in dataJSON) {
                tasksArr.push(Array(
                    (dataJSON[i].id),   
                    //('<a class="">'+dataJSON[i].status+'</a>' || ''),
                    (jobStatuses[dataJSON[i].status] ? '<div class="status '+jobStatuses[dataJSON[i].status]+'">'+dataJSON[i].status+'</div>' : 'Unknown'),     
                    (dataJSON[i].name || ''),
                    (dataJSON[i].application || ''),
                    (dataJSON[i].backend || ''),
                    (dataJSON[i].actualCE || '')
                    /*('<div class="wrapColumn" title="'+dataJSON[i].SchedulerJobId+'">'+dataJSON[i].SchedulerJobId+'</div>' || 'default'),
                    (dataJSON[i].TaskJobId || '0'),
                    (jobStatuses[dataJSON[i].STATUS] ? '<div class="status '+jobStatuses[dataJSON[i].STATUS]+'">'+jobStatuses[dataJSON[i].STATUS]+'</div>' : 'Unknown'),
                    (dataJSON[i].JobExecExitCode || '0'),
                    (dataJSON[i].GridEndId || '0'),
                    (dataJSON[i].resubmissions || '0'),
                    ('<a href="http://dashb-ssb.cern.ch/dashboard/request.py/sitehistory?site='+dataJSON[i].Site+'">'+dataJSON[i].Site+'</a>' || '0'),
                    (dataJSON[i].submitted || '0'),
                    (dataJSON[i].started || '0'),
                    (dataJSON[i].finished || '0')
                    */  
                ));
            }
            return tasksArr;
        },
        'charts': [
            {
                'ajax':true,
                'dataURL': 'http://localhost/?list=subjobs_statuses',
                'dataURL_params': function(Data) { 
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj; 
                },
                // translates data onto requires format:
                // {"chd":"t:60,40","chl":"Hello|World"}
                'translateData':function(dataJSON) {    
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Subjobs status',
                    'cht':'p3',
                    'chs':'350x130'
                }
            },
            /*{
                'ajax':true,
                'dataURL': 'http://localhost/?list=subjobs_backends',
                'dataURL_params': function(Data) {
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj; 
                },

                'translateData':function(dataJSON) {    
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Subjobs backend',
                    'cht':'p3',
                    'chs':'400x150'
                }
            },
            {
                'ajax':true,
                'dataURL': 'http://localhost/?list=subjobs_applications',
                'dataURL_params': function(Data) {
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj; 
                },

                'translateData':function(dataJSON) {
                        
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Subjobs application',
                    'cht':'p3',
                    'chs':'400x150'
                    //custom colors
                    //'chco':'3072F3|008000'
                }
            },*/
            {
                'ajax':true,
                'dataURL': 'http://localhost/?list=subjobs_actualCE',
                'dataURL_params': function(Data) {
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj; 
                },

                'translateData':function(dataJSON) {
                        
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Subjobs actualCE',
                    'cht':'p3',
                    'chs':'350x130'
                    //custom colors
                    //'chco':'3072F3|008000'
                }
            },
            {
                'ajax':true,
                'dataURL': 'http://localhost/?list=subjobs_accumulate',
                'dataURL_params': function(Data) {
                        obj = {
                        'taskmonid':Data.tid,
                        'from':Data.ts2iso(Data.from,2),
                        'to':Data.ts2iso(Data.till,3),
                        'timerange':Data.timeRange
                        };

                    return obj; 
                },

                'translateData':function(dataJSON) {
                    return dataJSON;
                },
                'gChart': {
                    'chxp':'0,0,100',
                    'chxt':'x,y',
                    'chs':'350x130',
                    'cht':'lxy',
                    'chco':'00FF00',
                    'chg':'9,9,1,6',
                    'chtt': 'Succeeded subjobs in time',
                    //'chd':'t:10,20,40,80,90,95,99|20,30,40,50,60,70,80',
                    'chm':'B,EFEFEF,0,0,0'// color
                }
            }
/*
,
            {
                'dataURL': 'http://localhost/?list=testaccumulation',
                'dataURL_params': function(Data) { return {'taskmonid':Data.tid}; },
                'translateData':function(dataJSON) {
                                function getDate(stringdate)
                                {
                                        array = stringdate.split(' ');
                                        big = array[0].split('-');
                                        small = array[1].split(':');
                       
                                        d = new Date(big[0], big[1] - 1, big[2], small[0], small[1], small[2]);
                                        return d;
                                }

                                function getWholePart(num)
                                {
                                        if(num.indexOf(".") > -1);
                                        {
                                                num = num.substring(0, num.indexOf("."));
                                        }

                                        return num;
                                }

                                succjobstotal = dataJSON.succjobs[0][0]['TOTAL'];
                                succjobstotalevents = dataJSON.succjobs[0][0]['TOTALEVENTS'];
                                totaljobs = dataJSON.totaljobs[0][0]['TOTAL'];

                                startdatestring = dataJSON.firststarted[0][0]['started'];
                                enddatestring = dataJSON.lastfinished[0][0]['finished'];

                                startdate = getDate(startdatestring);
                                enddate = getDate(enddatestring);

                                starttimestamp = startdate.getTime()/1000;
                                endtimestamp = enddate.getTime()/1000;
                       
                                interval_seconds = endtimestamp - starttimestamp;
                                total_number_finished_events = 0;
                       
                                var finished_event_time = Array();
                                var finished_events_number = Array();

                                for (event in dataJSON.allfinished[0])
                                {      
                                        finished_date = dataJSON.allfinished[0][event]['finished']      ;                      
                                        timestamp = getDate(finished_date).getTime()/1000;

                                        seconds_from_start_interval = timestamp - starttimestamp;

                                        total_number_finished_events += dataJSON.allfinished[0][event]['Events'];
                               
                                        x_scale = seconds_from_start_interval/interval_seconds*100;
                                        x_scale_str = x_scale + '';
                               
                                        if(x_scale_str.indexOf(".") > -1)
                                        {
                                                x_scale_int = x_scale_str.substring(0, x_scale_str.indexOf("."));
                                                finished_event_time.push(x_scale_int);
                                        }
                                        else
                                        {
                                                finished_event_time.push(x_scale);
                                        }

                                        finished_events_number.push(dataJSON.allfinished[0][event]['Events']);
                               
                                }

                                var accumulated_finished_events_number = Array();
                                for (event in finished_events_number)
                                {
                                        sum = 0;

                                        for (i=0;i<=event; i++)
                                        {
                                                sum += finished_events_number[i];
                                        }

                                        accumulated_finished_events_number[event] = sum;
                                }

                                for (event in accumulated_finished_events_number)
                                {
                                        accumulated_finished_events_number[event] = accumulated_finished_events_number[event]/total_number_finished_events*100;
                                }

                                eventsInTotal = succjobstotalevents/succjobstotal*totaljobs + '';

                                eventsInTotal = getWholePart(eventsInTotal);
                                mins = interval_seconds/60 + '';
                                minutes = getWholePart(mins);

                                var numberPointsShowed = 20;
                                scale = getWholePart(finished_event_time.length/numberPointsShowed + '');

                                var reduced_finished_event_time = Array();
                                var reduced_accumulated_finished_events_number = Array();

                                for (i = 0; i < finished_event_time.length; i++)
                                {
                                        if (i%scale == 0)
                                        {
                                                reduced_finished_event_time.push(finished_event_time[i]);
                                                reduced_accumulated_finished_events_number.push(accumulated_finished_events_number[i]);
                                        }
                                }

                                reduced_finished_event_time.push(finished_event_time[finished_event_time.length-1]);
                                reduced_accumulated_finished_events_number.push(accumulated_finished_events_number[finished_event_time.length-1]);
                                
                                var output = {
                                        'chxl':'0:|' + startdatestring + '|' + enddatestring,
                                        'chd':'t:0,' + reduced_finished_event_time.join(',') + '|0,' + reduced_accumulated_finished_events_number.join(','),
                                        'chxr':'1,0,' + total_number_finished_events,
                                        'chtt':'' + succjobstotalevents + ' processed events out of ' + eventsInTotal + ' in total. time interval - ' + minutes + ' minutes.'
                    };
                    return output;
                },
                'gChart': {
                    'chxp':'0,0,100',
                    'chxt':'x,y',
                    'chs':'600x300',
                    'cht':'lxy',
                    'chco':'00FF00',
                    'chg':'9,9,1,6',
                    //'chd':'t:10,20,40,80,90,95,99|20,30,40,50,60,70,80',
                    'chm':'B,EFEFEF,0,0,0'// color
                }
            }*/
        ]

    };
    // Task Jobs settings - FINISH
}

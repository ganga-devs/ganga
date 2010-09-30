// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 28.05.2010 Created
//

function Settings() {
    // Users list settings - START
    this.Users = {
        'dataURL': 'http://localhost/?list=users',
        'searchLabel': 'Search for user ',
        'translateData': function(dataJSON) {
            var usersList = Array();
            for (i in dataJSON) {
                usersList.push(dataJSON[i].GridName);//.replace(/"/g, ''));
            }
            return usersList;
        },
    };
    // Users list settings - FINISH
    
    // User Tasks settings - START
    this.Tasks = {
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
        'expandData': function(dataSet) {
            var outputArr = [
                ['inputdir', dataSet.inputdir],
                ['outputdir', dataSet.outputdir],
                ['uuid', dataSet.uuid]
            ];
            return outputArr;
        },
        'sorting':[1,'desc'],
        'iDisplayLength': 25,
        'tblLabels': ['id','status','name', 'application','backend','subjobs','submitted','running','completed','failed','actualCE'],
        'aoColumns': [
            {"sWidth":"80px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sClass":"numericTD","sWidth":"70px"},
            {"sClass":"numericTD","sWidth":"70px"},
            {"sClass":"numericTD","sWidth":"70px"},
            {"sClass":"numericTD","sWidth":"70px"},
            {"sClass":"numericTD","sWidth":"70px"},     
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
                tasksArr.push(Array(
                    //('<div class="wrapColumn" title="'+dataJSON[i].id+'"><a class="tmIdClick">'+dataJSON[i].id+'</a></div>' || 'default'),    
                    (dataJSON[i].id),   
                    //('<a class="">'+dataJSON[i].status+'</a>' || ''),
		    (jobStatuses[dataJSON[i].status] ? '<div class="status '+jobStatuses[dataJSON[i].status]+'">'+dataJSON[i].status+'</div>' : 'Unknown'),	
                    ('<a class="">'+dataJSON[i].name+'</a>' || ''),
                    ('<a class="">'+dataJSON[i].application+'</a>' || ''),
                    ('<a class="">'+dataJSON[i].backend+'</a>' || ''),
		    (dataJSON[i].subjobs),   
		    (dataJSON[i].submitted),   
		    (dataJSON[i].running),   
		    (dataJSON[i].completed),   
		    (dataJSON[i].failed),   
                    //('<a class="noJobsClick">'+dataJSON[i].subjobs+'</a>' || '0'),
                    //('<a class="">'+dataJSON[i].submitted+'</a>' || '0'),
                    //('<a class="noRunnClick">'+dataJSON[i].running+'</a>' || '0'),
                    //('<a class="">'+dataJSON[i].completed+'</a>' || '0'),
                    //('<a class="noFailClick">'+dataJSON[i].failed+'</a>' || '0'),
                    ('<a class="">'+dataJSON[i].actualCE+'</a>' || '')
                ));
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
            Data.tid = Data.mem.tasks.data[aPos[0]].id; 
        },
    };
    // User Tasks settings - FINISH
    
    // Task Jobs settings - START
    this.Jobs = {
        'dataURL': 'http://localhost/?list=subjobs',
        'dataURL_params': function(Data) {
            obj = {
                'taskmonid':Data.tid,
                'what':(Data.uparam[0] || 'all')
            };
            return obj;
        },
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
                    ('<a class="">'+dataJSON[i].name+'</a>' || ''),
                    ('<a class="">'+dataJSON[i].application+'</a>' || ''),
                    ('<a class="">'+dataJSON[i].backend+'</a>' || ''),
                    ('<a class="">'+dataJSON[i].actualCE+'</a>' || '')
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
    };
    // Task Jobs settings - FINISH
}

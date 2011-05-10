// This file is part of the jTaskMonitoring software
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
        'userSelection':true, // Display user selection page? (true|false)
        'jsonp': false,
        'pageTitle': 'Task Monitoring', // Page title
        'footerTxt': 'jTaskMonitoring \u00A9 2010', // Footer text
        'supportLnk': 'https://twiki.cern.ch/twiki/bin/view/ArdaGrid/TaskMonitoringWebUI', // Link to support page
        'logoLnk': 'media/images/dianelogo.png', // Link to page logo
        'usersListLbl': 'Users List',
        'mainsLbl': 'Masters',
        'subsLbL': 'Worker Nodes',
        'modelDefaults': function() { // Here You can set up model (data.js) default values
            return {
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
        }
    };
    // Application specific settings - FINISH

    // Users list settings - START
    this.Users = {
        'dataURL': 'http://dianemon.cern.ch/gangamon/get_users_JSON?application=diane', // Users list URL for ajax request
        //'dataURL': 'http://127.0.0.1:8000/gangamon/get_users_JSON', // Users list URL for ajax request
        'dataURL_params': function(Data) {
            obj = {
                'application':Data.application
            };
            return obj;
        },
        'searchLabel': 'Search for user ', // Label of the search field
        // Function, translates ajax response onto searchable list plugin data format
        // Output: [user1, user2, ...]
        'translateData': function(dataJSON) {
            return dataJSON;
        }
    };
    // Users list settings - FINISH
    
    // Mains settings - START
    this.Mains = {
        // Local, should be changed 
        'dataURL': 'http://dianemon.cern.ch/gangamon/get_runs_JSON', //'http://pcadc01.cern.ch/dashboard/request.py/gangataskstable',
	//'dataURL': 'http://127.0.0.1:8000/gangamon/get_runs_JSON', //'http://pcadc01.cern.ch/dashboard/request.py/gangataskstable',
        // The form things on the top of the dashboard
        'dataURL_params': function(Data) {
            obj = {
                'username':Data.user,
                'from':Data.ts2iso(Data.from,2),
                'to':Data.ts2iso(Data.till,3),
                'timerange':Data.timeRange
            };
            return obj;
        },
        // Shall the rows be expandable? (recommended true)
        'expandableRows':true,
        // Can more than one row be expanded at once?
        'multipleER':false,
        // Expanded data view setub object
        'expandData': {
            // Function, definition of data that will be displayed after row expansion
            // Input: 
            //  - rowDataSet - clicked row data (from ajax datatable response)
            //  - jsonDataSet - data extracted from ajax response
            // Output: {
            //   'properties':[[<property_name>,'<property_value>']] or false,
            //   'table':{
            //     'tblLabels':[<label1>,<label2>,...] or false,
            //     'tblData':[[<row1value1>,<row1value2>,...],[<row2value1>,<row2value2>,...],...]
            //   } or false,
            //   'html':<custom_html> or false
            // }
            'dataFunction': function(rowDataSet, jsonDataSet) {
                var properties = false;
                var html = false;
                var table = false;
                
                var properties = [
                    ['Master UUID',rowDataSet.master_uuid],
                    ['Run ID',rowDataSet.runid],
                    ['Host',rowDataSet.host],
                    ['Ganga Jobs',"<a href='http://dianemon.cern.ch/ganga.html'>Ganga Jobs</a>"] // Fix a more specific URL
                ];
                return {'properties':properties,'table':table,'html':html};
            }
        },
        // Sorts on the first row (first after the +, because of the expandable rows), descending
        'sorting':[2,'desc'],
        // How many rows shall be displayed at a time
        'iDisplayLength': 25,
        // Column names
        'tblLabels': ['Run ID','Start Time','Name','Application','User','Workers Total','Workers Now','Tasks Total','Tasks Completed'],
        // Column width and CSS stuff
        'aoColumns': [
            {"sClass":"numericTD","sWidth":"50px","sType": "integer-in-tag"},
            {"sWidth":"280px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sClass":"numericTD","sWidth":"110px","sType": "integer-in-tag"},
            {"sClass":"numericTD","sWidth":"110px","sType": "integer-in-tag"},
            {"sClass":"numericTD","sWidth":"110px","sType": "integer-in-tag"},
            {"sClass":"numericTD","sWidth":"110px","sType": "integer-in-tag"}
        ],
        // Magic
        'getDataArray': function(data) {
            return data;
        },
        // Goes through the JSON, and creates row for row with linkable information (or 0 as default)
        'translateData': function(dataJSON) {
            var tasksArr = Array();
            for (i in dataJSON) {
                tasksArr.push(Array(
                    // Add the rid in a wrapcolumn, in case it is too long
                    //('<div class="wrapColumn" style="width: 50px;" title="'+dataJSON[i].rid+'"><a>'+dataJSON[i].rid+'</a></div>' || 'default'),  // Ugly hack with the inline CSS, because the default is 350px, which takes up far too much space
                    (dataJSON[i].rid || '0'),	
                    (dataJSON[i].start_time || '0'),
                    ('<a class="drilldown taskNameClick">'+dataJSON[i].name+'</a>' || '0'),
                    (dataJSON[i].application || '0'),
                    (dataJSON[i].user || '0'),
                    (dataJSON[i].wn_total || '0'),
                    (dataJSON[i].wn_now || '0'),
                    (dataJSON[i].tasks_total || '0'),
                    (dataJSON[i].tasks_completed || '0')
                ));
            }
            return tasksArr;
        },
        'drillDownHandler': function(Data, el, rowIndex) {
            var classTranslate = {
                'taskNameClick':'all'
            };
            var uparam = [classTranslate[$(el).find('a').attr('class').replace("drilldown ","")]];
            //var tid = Data.mem.mains.data[rowIndex].TASKMONID;
            var tid = Data.mem.mains.data[rowIndex].master_uuid;
            return {'tid':tid,'uparam':uparam};
        }
    };
    // User Tasks settings - FINISH
    
    // Subs settings - START
    this.Subs = {
        'dataURL': 'http://dianemon.cern.ch/gangamon/get_tasks_JSON',
        //'dataURL': 'http://127.0.0.1:8000/gangamon/get_tasks_JSON',
        'dataURL_params': function(Data) {
            obj = {
                'taskmonid':Data.tid,
		'from':Data.ts2iso(Data.from,2),
                'to':Data.ts2iso(Data.till,3),
                'timerange':Data.timeRange
            };
            return obj;
        },
        'expandableRows':false,
        'multipleER':false,
        // Expanded data view setub object
        'expandData': {
            // Function, definition of data that will be displayed after row expansion
            // Input: 
            //  - rowDataSet - clicked row data (from ajax datatable response)
            //  - jsonDataSet - data extracted from ajax response
            // Output: {
            //   'properties':[[<property_name>,'<property_value>']] or false,
            //   'table':{
            //     'tblLabels':[<label1>,<label2>,...] or false,
            //     'tblData':[[<row1value1>,<row1value2>,...],[<row2value1>,<row2value2>,...],...]
            //   } or false,
            //   'html':<custom_html> or false
            // }
            'dataFunction': function(rowDataSet, jsonDataSet) {
                var properties = false;
                var html = false;
                var table = false;
                
                var properties = Array();
                // Send an empty list if there is no actual data
                // 'Unknown' is a result of addQuotes in views.py, where it is the default value if nothing else is given
                if (rowDataSet.task_labels == 'Unknown') {
                    return {'properties':properties,'table':table,'html':html};
                }
                task_labels = rowDataSet.task_labels.split('!@#$');
                for (i in task_labels) {
                    label = task_labels[i].split('#!$!#');
                    properties.push(Array(label[0], label[1]));  // label[0] is the label part, label[0] is the value part
                }
                return {'properties':properties,'table':table,'html':html};
            }
        },
        'sorting':[4,'asc'],
        'iDisplayLength': 25,
        'tblLabels': ['Task ID','Status','Execution Count','Application Label','Worker ID'],
        'aoColumns': [
            {"sClass":"numericTD","sWidth":"50px","sType": "integer-in-tag"},
            null,
            {"sClass":"numericTD","sType": "integer-in-tag"},
            {"sWidth":"400px"},
            {"sClass":"numericTD","sType": "integer-in-tag"}
        ],
        'getDataArray': function(data) {
            return data;
        },
        'translateData': function(dataJSON) {
            var jobStatuses = {
                'new':'Pending',
                'running':'Running',
                'completed':'Successfull',
		'incomplete':'NotCompleted',
                'failed':'Failed'
            };

            var jobsArr = Array();
            for (i in dataJSON) {
                jobsArr.push(Array(
                    ('<div class="wrapColumn" style="width: 50px" title="'+dataJSON[i].tid+'">'+dataJSON[i].tid+'</div>' || 'default'),

                    //('<a>'+dataJSON[i].run || '0'),
                    (jobStatuses[dataJSON[i].status] ? '<div class="status '+jobStatuses[dataJSON[i].status]+'">'+dataJSON[i].status+'</div>' : 'Unknown'),
                    //(dataJSON[i].status || '0'),
                    (dataJSON[i].execution_count || '0'),
                    ('<div class="wrapColumn" style="width: 400px" title="'+dataJSON[i].application_label+'">'+dataJSON[i].application_label+'</div>' || ''),
                    (dataJSON[i].wid || '0')
                ));
            }
            return jobsArr;
        },
        'setupUserParams': function(Data, el, aPos) {
            Data.tid = Data.mem.runs.data[aPos[0]].TASKMONID;
        },
	'charts': [
            {
                'ajax':true,
		'dataURL': 'http://dianemon.cern.ch/gangamon/get_tasks_statuses',
                //'dataURL': 'http://127.0.0.1:8000/gangamon/get_tasks_statuses',
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
  		    //return {"chd":"t:60,40","chl":"Hello|World"};	
                    return dataJSON;
                },
                'gChart': {
                    'chtt':'Tasks status',
                    'cht':'p3',
                    'chs':'350x130'
                }
            }
	]
    };
    // Task Jobs settings - FINISH
}

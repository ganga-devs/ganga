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
        'userSelection': true, // Display user selection page? (true|false)
        'jsonp': false,
        'pageTitle': 'Task Monitoring', // Page title
        'footerTxt': 'jTaskMonitoring \u00A9 2010', // Footer text
        'supportLnk': 'https://twiki.cern.ch/twiki/bin/view/ArdaGrid/TaskMonitoringWebUI', // Link to support page
        'logoLnk': 'media/images/gangalogo.png', // Link to page logo
        'usersListLbl': 'Users List',
        'mainsLbl': 'Jobs',
        'subsLbL': 'Sub-jobs',
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
        'dataURL': 'http://gangamon.cern.ch/gangamon/get_users_JSON?application=ganga', // Users list URL for ajax request
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
        'dataURL': 'http://gangamon.cern.ch/gangamon/gangajobs', //'http://pcadc01.cern.ch/dashboard/request.py/gangataskstable',
        // The form things on the top of the dashboard
        'dataURL_params': function(Data) {
            obj = {
                'user':Data.user
            };
            if (Data.from) obj['from'] = Data.from;
            if (Data.from) obj['till'] = Data.till;
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
                    ['Job UUID',rowDataSet[8]]
                ];
                return {'properties':properties,'table':table,'html':html};
            }
        },
        // Sorts on the first row (first after the +, because of the expandable rows), descending
        'sorting':[1,'desc'],
        // How many rows shall be displayed at a time
        'iDisplayLength': 25,
        // Column names
        'tblLabels': ['Time','User','Id','Subjobs','Status','Application','Backend','Execution Host','Name'],
        // Column width and CSS stuff
        /*'aoColumns': [
            {"sClass":"numericTD","sWidth":"50px"},
            {"sWidth":"180px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sWidth":"110px"},
            {"sClass":"numericTD","sWidth":"110px"},
            {"sClass":"numericTD","sWidth":"110px"}
        ],*/
        // Magic
        'getDataArray': function(data) {
            return data;
        },
        // Goes through the JSON, and creates row for row with linkable information (or 0 as default)
        'translateData': function(dataJSON) {
           var jobStatuses = {
                'submitted':'Submitted',
                'running':'Running',
                'failed':'Failed',
                'finished':'Finished',
                'unknown':'Unknown'
            };
            var tasksArr = Array();
            for (i in dataJSON) {
                //if there are more than 0 subjobs
                if (dataJSON[i][3] > 0)
                    {
                    tasksArr.push(Array('<b>'+dataJSON[i][0]+'</b>',dataJSON[i][1],dataJSON[i][2],'<a class="drilldown subJobClick">'+dataJSON[i][3]+'</a>',
                    (jobStatuses[dataJSON[i][4]] ? '<div class="status ' + jobStatuses[dataJSON[i][4]]+'">'+dataJSON[i][4]+'</div>' : '<div class="status Unknown">Unknown</div>'),
                    dataJSON[i][5],dataJSON[i][6],'',dataJSON[i][10]));
                    }
                else
                    {
                    tasksArr.push(Array('<b>'+dataJSON[i][0]+'</b>',dataJSON[i][1],dataJSON[i][2],'',
                    (jobStatuses[dataJSON[i][4]] ? '<div class="status ' + jobStatuses[dataJSON[i][4]]+'">'+dataJSON[i][4]+'</div>' : '<div class="status Unknown">Unknown</div>'),
                    dataJSON[i][5],dataJSON[i][6],dataJSON[i][7],dataJSON[i][10]));
                    }
            }
            return tasksArr;
        },
        'drillDownHandler': function(Data, el, rowIndex) {
            var classTranslate = {
                'subJobClick':'all'
            }; 
            var uparam = [classTranslate[$(el).find('a').attr('class').replace("drilldown ","")]];
            //var tid = Data.mem.mains.data[rowIndex].TASKMONID;
            var tid = Data.mem.mains.data[rowIndex][8];
            return {'tid':tid,'uparam':uparam};
        }
    };
    // User Tasks settings - FINISH
    // Subs settings - START
    this.Subs = {
        'dataURL': 'http://gangamon.cern.ch/gangamon/gangadetails',
        'dataURL_params': function(Data) {
            obj = {
                'job_uuid':Data.tid
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
                
                return {'properties':properties,'table':table,'html':html};
            }
        },
        'sorting':[1,'asc'],
        'iDisplayLength': 25,
        'tblLabels': ['Time','Id','Status','Application','Backend','Execution Host','Name'],
        /*'aoColumns': [
            {"sClass":"numericTD","sWidth":"50px"},
            {"sClass":"numericTD","sWidth":"60px"},
            {"sWidth":"60px"},
            {"sWidth":"80px"},
            {"sClass":"numericTD","sWidth":"50px"},
            {"sWidth":"200px"}
        ],*/
        'getDataArray': function(data) {
            return data;
        },
        'translateData': function(dataJSON) {
           var jobStatuses = {
                'submitted':'Submitted',
                'running':'Running',
                'failed':'Failed',
                'finished':'Finished',
                'unknown':'Unknown'
            };
            var tasksArr = Array();
            for (i in dataJSON.subjobs) {
                tasksArr.push(Array('<b>'+dataJSON.subjobs[i][0]+'</b>',dataJSON.subjobs[i][1],
                (jobStatuses[dataJSON.subjobs[i][2]] ? '<div class="status ' + jobStatuses[dataJSON.subjobs[i][2]]+'">'+dataJSON.subjobs[i][2]+'</div>' : '<div class="status Unknown">Unknown</div>'),
                dataJSON.subjobs[i][3],dataJSON.subjobs[i][4],dataJSON.subjobs[i][5],dataJSON.subjobs[i][7]));
            }
            return tasksArr;
        },
        'setupUserParams': function(Data, el, aPos) {
            Data.tid = Data.mem.runs.data[aPos[0]].TASKMONID;
        }
    };
    // Task Jobs settings - FINISH
}

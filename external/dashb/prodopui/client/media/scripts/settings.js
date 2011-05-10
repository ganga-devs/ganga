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
        'userSelection': false, // Display user selection page? (true|false)
        'dataRefresh': true, // Display refresh dropdown field? (true|false)
        'timeRangeSelection': true, // Display time range dropdown field? (true|false)
        'jsonp': false, // allow requests to other hosts
        'pageTitle': 'Production Operators UI', // Page title
        'footerTxt': 'Production Operators UI', // Footer text
        'supportLnk': 'https://twiki.cern.ch/twiki/bin/view/ArdaGrid/TaskMonitoringWebUI', // Link to support page
        'logoLnk': 'media/images/atlaslogo.png', // Link to page logo
        'usersListLbl': 'Users List', // Label of user list search field, example: 'Users List'
        'mainsLbl': 'Tasks', // Name of mains content, example: 'Tasks'
        'subsLbL': 'Jobs', // Name of subs content, example: 'Jobs'
        'debugMode': true, // Display debug messages on errors inside dataTranslate functions? (true|false)
        'modelDefaults': function() { // Here You can set up model (data.js) default values
            return {
                'user': 'default',
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
        'dataURL': '', // Users list URL for ajax request
        // Function, ajax request parameters
        // Output: {'<parameter_name>':<parameter_value>,...} (default: {})
        /*'dataURL_params': function(Data) {
            return {};
        },*/
        'searchLabel': 'Search for user ', // Label of the search field
        // Function, translates ajax response onto searchable list plugin data format
        // Output: [user1, user2, ...]
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
    
    // Mains settings - START
    this.Mains = {
        'dataURL': '/dashboard/request.py/tasklistprod', // Mains URL for ajax request
        // Function, ajax request parameters
        // Input: Data - application Data model, rowDataSet - clicked row data (from ajax datatable response)
        // Output: {'<parameter_name>':<parameter_value>,...} (default: {})
        'dataURL_params': function(Data, rowDataSet) {
            obj = {
                //'usergridname':Data.user,
                'from':Data.ts2iso(Data.from,2),
                'to':Data.ts2iso(Data.till,3),
                'timerange':((Data.filters.notmodsince == '') ? Data.timeRange : ''),
                'pattern':Data.filters.pattern,
                'typeofproc':Data.filters.typeofproc,
                'site':Data.filters.site,
                'cloud':Data.filters.cloud,
                'notmodsince':Data.filters.notmodsince
            };
            return obj;
        },
        'expandableRows':true, // If TRUE, rows will expand after clicking '+'
        'multipleER':false, // If TRUE, multiple rows can be expanded
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
                var table = false;
                var charts = false;
                
                var dErrNum = function(eNum) {
                    if (eNum > 0) return '<p style="color:red;font-weight:bold;margin:0px" class="numericTD">'+eNum+'</p>';
                    else return '<p style="color:#32CD32;font-weight:bold;margin:0px" class="numericTD">'+eNum+'</p>';
                };
                
                properties = [
                    ['TaskName', rowDataSet.TASKNAME],
                    ['TaskId', rowDataSet.PRODTASKID],
                    ['TaskMonitorId', rowDataSet.TaskMonitorId],
                    ['TaskCreatedTimeStamp', rowDataSet.TaskCreatedTimeStamp],
                    ['Application', rowDataSet.Application],
                    //['ApplExecId',rowDataSet.ApplExecId],
                    ['ModificationTime',rowDataSet.ModificationTime],
                    ['InputCollection', rowDataSet.InputCollection.replace(/,/g, ', ')],
                    ['DestCloud',rowDataSet.DestCloud],
                    ['Site',rowDataSet.Site],
                    ['InputDataFiles',rowDataSet.NINPUTDATAFILES],
                    ['InputFileType',rowDataSet.INPUTFILETYPE],
                    ['InputFileBytes',rowDataSet.INPUTFILEBYTES],
                    ['InputFileProject',rowDataSet.INPUTFILEPROJECT],
                    ['WorkingGroup',rowDataSet.WorkingGroup],
                ];
                
                table = {
                    tblLabels:['', 'TaskBuffer','Supervisor','JobDispather','Transformation','Pilot','DDM','BrokerAge','Execution'],
                    tblData:[
                        ['<span style="font-weight:bold">Current</span>', dErrNum(rowDataSet.TASKBUFFER), dErrNum(rowDataSet.SUPERVISOR), dErrNum(rowDataSet.JOBDISPATCHER), dErrNum(rowDataSet.TRANSFORMATION), dErrNum(rowDataSet.PILOT), dErrNum(rowDataSet.DDM), dErrNum(rowDataSet.BROKERAGE), dErrNum(rowDataSet.EXECUTION)],
                        ['<span style="font-weight:bold">Toral</span>', dErrNum(rowDataSet.T_TASKBUFFER), dErrNum(rowDataSet.T_SUPERVISOR), dErrNum(rowDataSet.T_JOBDISPATCHER), dErrNum(rowDataSet.T_TRANSFORMATION), dErrNum(rowDataSet.T_PILOT), dErrNum(rowDataSet.T_DDM), dErrNum(rowDataSet.T_BROKERAGE), dErrNum(rowDataSet.T_EXECUTION)]
                    ]
                };
                
                // setting up charts - start
                charts = [{
                    'name':'Job Evolution',
                    'type':'hchart', // (gchart|hchart)
                    'onDemand':true,
                    'dataURL': '/dashboard/request.py/jobevol',
                    'dataURL_params':function(Data) {
                        return {'taskid':rowDataSet.TASKID};
                    },
                    // translates data onto requires format:
                    // {"chd":"t:60,40","chl":"Hello|World"}
                    'translateData':function(jsonDataSet) {
                        var jobevolArr = jsonDataSet.jobevol[0];
                        var tickInterval = null, correction = 0, subtitle = '';
                        
                        if (jobevolArr.length > 0) {
                            jobevolArr.sort(function(a,b){return (a.ENDTIME - b.ENDTIME)});
                            
                            if (jsonDataSet.jobevol[1].granularity == 'HH24') {
                                tickInterval = 3600*1000;
                                correction = 3600/2;
                                subtitle = 'Hourly';
                            } else if (jsonDataSet.jobevol[1].granularity == 'DD') {
                                tickInterval = 24*3600*1000;
                                correction = 3600*12;
                                subtitle = 'Daily';
                            }
                        
                            var minTime = jobevolArr[0].ENDTIME;
                            var maxTime = jobevolArr[(jobevolArr.length-1)].ENDTIME;
                            var pointsCnt = ((maxTime-minTime)/(tickInterval/1000))+1;
                            
                            var statusesArr = ['FINISHED','FAILED'];
                            var series = [{name:'Finished',color:'#32CD32',data:[]},{name:'Failed',color:'#FF0000',data:[]}];
                            
                            var currentTime = minTime;
                            var j = 0;
                            for (var i=0;i<pointsCnt;i++) {
                                var x = ((currentTime+correction)*1000);
                                var y1 = 0, y0 = 0;
                                if (currentTime == jobevolArr[j].ENDTIME) {
                                    y1 = jobevolArr[j].FINISHED;
                                    y0 = jobevolArr[j].FAILED;
                                    j++;
                                }
                                series[0].data.push({y:y1,x:x});
                                series[1].data.push({y:y0,x:x});
                                currentTime += (tickInterval/1000);
                            }
                        } else return false;
                        
                        options = {
                            chart: {
                                renderTo: 'container',
                                defaultSeriesType: 'column',
                                height:300
                            },
                            title: {text: 'Job Evolution'},
                            subtitle: {text: subtitle},
                            xAxis: {
                                type: "datetime",
                                startOnTick: true,
                                endOnTick: true,
                                showLastLabel: true,
                                tickInterval:tickInterval,
                                dateTimeLabelFormats: {
                                    second: '%Y-%m-%d %H:%M',
                                    minute: '%Y-%m-%d %H:%M',
                                    hour: '%Y-%m-%d %H:%M',
                                    day: '%Y-%m-%d %H:%M' 
                                },
                                labels: {
                                    rotation: -35,
                                    align: 'right',
                                    style: {
                                        font: "normal 9px 'Lucida Grande', 'Lucida Sans Unicode', Verdana, Arial, Helvetica, sans-serif"
                                    }
                                }
                            },
                            yAxis: {
                                min: 0,
                                allowDecimals: false,
                                gridLineWidth: 1,
                                title: {
                                    text: 'Jobs Number'
                                }
                            },
                            legend: {
                                align: 'right',
                                x: -70,
                                verticalAlign: 'top',
                                y: 20,
                                floating: true,
                                backgroundColor: '#FFFFFF',
                                borderColor: '#CCC',
                                borderWidth: 1,
                                shadow: false
                            },
                            tooltip: {
                                shared: true,
                                crosshairs: true
                            },
                            plotOptions: {
                                column: {
                                    stacking: 'normal',
                                    pointPadding:0,
                                    groupPadding:0,
                                    //pointWidth: '20'
                                }
                            },
                            series: series
                        }
                        return options;
                    }
                },{
                    'name':'Job Evolution Cumulative Plot',
                    'type':'hchart', // (gchart|hchart)
                    'onDemand':true,
                    'dataURL': '/dashboard/request.py/jobevol',
                    'dataURL_params':function(Data) {
                        return {'taskid':rowDataSet.TASKID};
                    },
                    // translates data onto requires format:
                    // {"chd":"t:60,40","chl":"Hello|World"}
                    'translateData':function(jsonDataSet) {
                        var jobevolArr = jsonDataSet.jobevol[0];
                        var tickInterval = null, correction = 0;
                        
                        if (jobevolArr.length > 0) {
                            jobevolArr.sort(function(a,b){return (a.ENDTIME - b.ENDTIME)});
                            
                            if (jsonDataSet.jobevol[1].granularity == 'HH24') {
                                tickInterval = 3600*1000;
                                correction = 0;//3600/2;
                            } else if (jsonDataSet.jobevol[1].granularity == 'DD') {
                                tickInterval = 24*3600*1000;
                                correction = 0;//3600*12;
                            }
                            
                            var statusesArr = ['FINISHED','FAILED'];
                            var series = [
                                {
                                    name:'Finished',
                                    color:'#32CD32',
                                    data:[]
                                },
                                {
                                    name:'Failed',
                                    color:'#FF0000',
                                    data:[]
                                }
                            ];
                            
                            var bufS = 0;
                            var bufF = 0;
                            for (var i=0;i<jobevolArr.length;i++) {
                                //alert('fin:'+jobevolArr[i].FINISHED+"\n"+'fail:'+jobevolArr[i].FAILED+"\n"+'endtime:'+jobevolArr[i].ENDTIME);
                                bufS += jobevolArr[i].FINISHED; bufF += jobevolArr[i].FAILED;
                                series[0].data.push({y:bufS,x:((jobevolArr[i].ENDTIME+correction)*1000)});
                                series[1].data.push({y:bufF,x:((jobevolArr[i].ENDTIME+correction)*1000)});
                            }
                        } else return false;
                        
                        options = {
                            chart: {
                                renderTo: 'container',
                                defaultSeriesType: 'areaspline',
                                height:300
                            },
                            title: {
                                text: 'Job Evolution Cumulative Plot'
                            },
                            xAxis: {
                                type: "datetime",
                                startOnTick: false,
                                endOnTick: false,
                                showLastLabel: true,
                                tickInterval:tickInterval,
                                dateTimeLabelFormats: {
                                    second: '%Y-%m-%d %H:%M',
                                    minute: '%Y-%m-%d %H:%M',
                                    hour: '%Y-%m-%d %H:%M',
                                    day: '%Y-%m-%d %H:%M' 
                                },
                                labels: {
                                    rotation: -35,
                                    align: 'right',
                                    style: {
                                        font: "normal 9px 'Lucida Grande', 'Lucida Sans Unicode', Verdana, Arial, Helvetica, sans-serif"
                                    }
                                }
                            },
                            yAxis: {
                                min: 0,
                                allowDecimals: false,
                                gridLineWidth: 1,
                                title: {
                                    text: 'Jobs Number'
                                }
                            },
                            legend: {
                                align: 'right',
                                x: -100,
                                verticalAlign: 'top',
                                y: 20,
                                floating: true,
                                backgroundColor: '#FFFFFF',
                                borderColor: '#CCC',
                                borderWidth: 1,
                                shadow: false
                            },
                            tooltip: {
                                shared: true,
                                crosshairs: true
                                /*formatter: function() {
                                    return this.series.name +': '+ this.y;
                                }*/
                            },
                            plotOptions: {
                                column: {
                                    stacking: 'normal'//,
                                    //pointWidth: '20'
                                }
                            },
                            series: series
                        }
                        return options;
                    }
                }];
                // setting up charts - finish
                
                return [['properties',properties],['charts',charts],['table',table]];
            }
        },
        'sorting':[4,'desc'], // [<column_index>,<sorting_direction>], sorting_direction='desc'||'asc'
        'iDisplayLength': 25, // Number of rows to display on single page
        // Column labels
        'tblLabels': ['Graphically','TaskName', 'TaskType', 'TaskID', 'Status',  'NTotal', 'PFail', 'Fin.', 'Fail.', 'Cancel.', 'Def.', 'Assi.', 'Wait.' ,'Sent', 'Activ.', 'Start.', 'Run.', 'Hold.' ,'Transf.', 'InputCollection', 'TaskMonitorId'],
        // dataTables plugin column options
        // see: http://www.datatables.net/usage/columns
        'aoColumns': [
            {"sWidth":"80px","bSortable":false},
            {"sWidth":"120px"},
            null,
            null,
            null,
            {"sClass":"numericTD"},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span title="#failed/(#failed + #finished)">'+oObj.aData[7]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#32CD32;font-weight:bold;">'+oObj.aData[8]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#FF0000;font-weight:bold;">'+oObj.aData[9]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#0000FF;font-weight:bold;">'+oObj.aData[10]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#FFFF00;font-weight:bold;">'+oObj.aData[11]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#FFCC00;font-weight:bold;">'+oObj.aData[12]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#C0C0C0;font-weight:bold;">'+oObj.aData[13]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#808080;font-weight:bold;">'+oObj.aData[14]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#800080;font-weight:bold;">'+oObj.aData[15]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#EE82EE;font-weight:bold;">'+oObj.aData[16]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#FF00FF;font-weight:bold;">'+oObj.aData[17]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#00FFFF;font-weight:bold;">'+oObj.aData[18]+'</span>';},"bUseRendered": false},
            {"sClass":"numericTD", "fnRender":function(oObj){return '<span style="color:#0000FF;font-weight:bold;">'+oObj.aData[19]+'</span>';},"bUseRendered": false},
            {"bVisible": false},
            {"bVisible": false}
            //{"sWidth":"130px","bSortable":false}
        ],
        // Function: extracting array of data form Ajax response
        // Example:
        // - Ajax response: {'user_taskstable':[{col_val1, col_val2, ...}, ...]}
        // - Required function: function(data) { return data.user_taskstable; }
        'getDataArray': function(data) {
            return data.tasksList;
        },
        // Function, translates ajax response onto dataTables plugin data format
        // Output: [[col_val1, col_val2, ...], ...]
        'translateData': function(dataJSON) {
            var tasksArr = Array();
            for (i in dataJSON) {
                tasksArr.push(Array(
                    '<img src="http://chart.apis.google.com/chart?chbh=a,0&chs=80x11&cht=bhs:nda&chco=32CD32,FF0000,0000FF,FFFF00,FFCC00,C0C0C0,808080,800080,EE82EE,FF00FF,00FFFF,0000FF&chds=0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+',0,'+dataJSON[i].NJOBTOTAL+'&chd=t:'+dataJSON[i].FINISHED+'|'+dataJSON[i].FAILED+'|'+dataJSON[i].CANCELLED+'|'+dataJSON[i].DEFINED+'|'+dataJSON[i].ASSIGNED+'|'+dataJSON[i].WAITING+'|'+dataJSON[i].SENT+'|'+dataJSON[i].ACTIVATED+'|'+dataJSON[i].STARTING+'|'+dataJSON[i].RUNNING+'|'+dataJSON[i].HOLDING+'|'+dataJSON[i].TRANSFERRING+'" />',
                    ('<div class="wrapColumn" style="width:120px" title="'+dataJSON[i].TASKNAME+'"><a class="drilldown tmIdClick">'+dataJSON[i].TASKNAME+'</a></div>' || 'default'),
                    (dataJSON[i].TASKTYPE || '0'),
                    (dataJSON[i].PRODTASKID || '0'),
                    (dataJSON[i].STATUS || '0'),
                    (dataJSON[i].NJOBTOTAL || '0'),
                    ('<span title="#failed/(#failed + #finished)">'+dataJSON[i].PFAIL.toFixed(3)+'</span>' || '0'),
                    (dataJSON[i].FINISHED || '0'),
                    (dataJSON[i].FAILED || '0'),
                    (dataJSON[i].CANCELLED || '0'),
                    (dataJSON[i].DEFINED || '0'),
                    (dataJSON[i].ASSIGNED || '0'),
                    (dataJSON[i].WAITING || '0'),
                    (dataJSON[i].SENT || '0'),
                    (dataJSON[i].ACTIVATED || '0'),
                    (dataJSON[i].STARTING || '0'),
                    (dataJSON[i].RUNNING || '0'),
                    (dataJSON[i].HOLDING || '0'),
                    (dataJSON[i].TRANSFERRING || '0'),
                    (dataJSON[i].InputCollection || ''),
                    (dataJSON[i].TaskMonitorId || '')
                    //'<img src="http://chart.apis.google.com/chart?chbh=a,0&chs=130x15&cht=bhs:nda&chco=59D118,C50000,3072F3,FF9900,C2BDDD&chds=0,'+dataJSON[i].NUMOFJOBS+',0,'+dataJSON[i].NUMOFJOBS+',0,'+dataJSON[i].NUMOFJOBS+',0,'+dataJSON[i].NUMOFJOBS+',0,'+dataJSON[i].NUMOFJOBS+'&chd=t:'+dataJSON[i].SUCCESS+'|'+dataJSON[i].FAILED+'|'+dataJSON[i].RUNNING+'|'+dataJSON[i].PENDING+'|'+dataJSON[i].UNKNOWN+'" />'
                ));
            }
            return tasksArr;
        },
        // Function, it is executed every time someone clicks cell with a.drilldown html tag in it
        // Main purpose of the function is to indicate tid and (optionary) uparam parameters from the Data object
        // (uparam allows to setup additional parameters to tid that would define a sub table ajax request)
        // This allows to properly display subs table
        // Input:
        //  - Data - application Data object
        //  - el - clicked jQuery element
        //  - rowIndex - index of the clicked row
        // Output: {
        //   'uparam':[<parameters_list>],
        //   'tid':<id_for_the_subtable>
        'drillDownHandler': function(Data, el, rowIndex) {
            var classTranslate = {
                'tmIdClick':'all',
                'noJobsClick':'all',
                'noPendClick':'P',
                'noRunnClick':'R',
                'noSuccClick':'S',
                'noFailClick':'F',
                'noUnknClick':'U'
            };
            var uparam = ['all'];
            var tid = Data.mem.mains.data[rowIndex].TaskMonitorId;
            
            return {'tid':tid,'uparam':uparam};
        },
        'filters':[
            {
                'label':'Pattern',  // String
                'urlVariable':'pattern',  // String - lower cased, no spaces, no special characters
                'fieldType':'text', // String (text|select|date)
                'value':'',
                'options':{}
            },
            {
                'label':'Type of processing',  // String
                'urlVariable':'typeofproc',  // String - lower cased, no spaces, no special characters
                'fieldType':'select',  // String (text|select|date)
                'value':'',
                'options':{
                    'dataURL':'/dashboard/request.py/inittaskprod?data=tasktype',  // (optional) String (url|false)
                    // Function translates model or ajax data onto simple elements array
                    // Input: data - data represents Data.mem object or ajax response depending on whether dataURL exists or not
                    // Output: [['el1','el1 label'],['el2','el2 label'], ...] - Can also be defined as a static list (when you don't want to
                    // load the data from url nor using Data.mem object)
                    'translateData': function(data) {
                        var sitesArr = data.basicData;
                        var output = [['','Off']];
                        
                        for (var i=0;i<sitesArr.length;i++) {
                            output.push([sitesArr[i].TASKTYPE,sitesArr[i].TASKTYPE]);
                        }
                        //alert(output);
                        return output;
                        //return [['','Off'],['el1','Element 1'],['el2','Element 2'],['el3','Element 3']];
                    }
                }
            },
            {
                'label':'Task status',  // String
                'urlVariable':'taskstatus',  // String - lower cased, no spaces, no special characters
                'fieldType':'select',  // String (text|select|date)
                'value':'',
                'options':{
                    //'dataURL':'#',  // (optional) String (url|false)
                    // Function translates model or ajax data onto simple elements array
                    // Input: data - data represents Data.mem object or ajax response depending on whether dataURL exists or not
                    // Output: [['el1','el1 label'],['el2','el2 label'], ...] - Can also be defined as a static list (when you don't want to
                    // load the data from url nor using Data.mem object)
                    'translateData': function(data) {
                        return [['','Off'],['defined','Defined'],['assigned','Assigned'],
                        ['waiting','Waiting'],['activated','Activated'],['starting','Starting'],
                        ['running','Running'],['holding','Holding'],['transferring','Transferring'],
                        ['finished','Finished'],['failed','Failed'],['cancelled','Cancelled']];
                    }
                }
            },
            {
                'label':'Site',  // String
                'urlVariable':'site',  // String - lower cased, no spaces, no special characters
                'fieldType':'select',  // String (text|select|date)
                'value':'',
                'options':{
                    'dataURL':'/dashboard/request.py/inittaskprod?data=sites',  // (optional) String (url|false)
                    // Function translates model or ajax data onto simple elements array
                    // Input: data - data represents Data.mem object or ajax response depending on whether dataURL exists or not
                    // Output: [['el1','el1 label'],['el2','el2 label'], ...] - Can also be defined as a static list (when you don't want to
                    // load the data from url nor using Data.mem object)
                    'translateData': function(data) {
                        var sitesArr = data.basicData;
                        var output = [['','Off']];
                        
                        for (var i=0;i<sitesArr.length;i++) {
                            output.push([sitesArr[i].SITENAME,sitesArr[i].SITENAME]);
                        }
                        
                        return output;
                        //return [['','Off'],['el1','Element 1'],['el2','Element 2'],['el3','Element 3']];
                    }
                }
            },
            {
                'label':'Cloud',  // String
                'urlVariable':'cloud',  // String - lower cased, no spaces, no special characters
                'fieldType':'select',  // String (text|select|date)
                'value':'',
                'options':{
                    //'dataURL':'#',  // (optional) String (url|false)
                    // Function translates model or ajax data onto simple elements array
                    // Input: data - data represents Data.mem object or ajax response depending on whether dataURL exists or not
                    // Output: [['el1','el1 label'],['el2','el2 label'], ...] - Can also be defined as a static list (when you don't want to
                    // load the data from url nor using Data.mem object)
                    'translateData': function(data) {
                        return [["","Off"],["CERN","CERN"], ["CA","CA"], ["DE","DE"], ["ES","ES"], ["FR","FR"], ["IT","IT"], 
                        ["ND","ND"], ["NL","NL"], ["TW","TW"], ["UK","UK"], ["US","US"], ["unknown","unknown"]];
                    }
                }
            },
            {
                'label':'Not modified since',  // String
                'urlVariable':'notmodsince',  // String - lower cased, no spaces, no special characters
                'fieldType':'datetime', // String (text|select|date)
                'value':'',
                'options':{
                    // On and Off functions are executed when filters submit is clicked
                    // On is executed when field has value other then empty string ("")
                    // Otherwise Off is executed
                    'On':function(Data) {  // Data is a Controller.Data object
                        $('#from,#till,#timeRange').attr('disabled',true);
                    },
                    'Off':function(Data) {  // Data is a Controller.Data object
                        $('#from,#till,#timeRange').attr('disabled',false);
                    }
                }
            }
        ]/*,
        'charts': []*/
    };
    // User Tasks settings - FINISH
    
    // Subs settings - START
    this.Subs = {
        'dataURL': '/dashboard/request.py/gangataskjobs', // Subs list URL for ajax request
        // Function, ajax request parameters
        // Output: {'<parameter_name>':<parameter_value>,...} (default: {})
        'dataURL_params': function(Data) {
            obj = {
                'taskmonid':Data.tid,
                'what':(Data.uparam[0] || 'all')
            };
            return obj;
        },
        'expandableRows':true, // If TRUE, rows will expand after clicking '+'
        'multipleER':true, // If TRUE, multiple rows can be expanded
        // Expanded data view setub object
        'expandData': {
            'dataURL': '/dashboard/request.py/resubmittedjobsAtl',
            'dataURL_params': function(Data, currentRow) {
                //alert(currentRow);
                obj = {
                    'what':'ALL',
                    'taskjobid':currentRow.TaskJobId,
                    'taskmonid':Data.tid,
                    'type':'prod'
                };
                return obj;
            },
            // Function, definition of data that will be displayed after row expansion
            // Input: 
            //  - rowDataSet - clicked row data (from ajax datatable response)
            //  - jsonDataSet - data extracted from ajax response
            // Output: {
            //   'properties':[[<property_name>,'<property_value>']] or false,
            //   'table':{
            //     'tblLabels':[<label1>,<label2>,...],
            //     'tblData':[[<row1value1>,<row1value2>,...],[<row2value1>,<row2value2>,...],...]
            //   } or false,
            //   'html':<custom_html> or false
            // }
            'dataFunction': function(rowDataSet, jsonDataSet) {
                if (rowDataSet.STATUS == 'F') var properties = [['Reason of Failure',(rowDataSet.AppGenericStatusReasonValue || 'None/Unknown')]];
                else var properties = false;
                var html = '<p style="margin:0px; color:#1E4A68; font-weight:bold;text-align:center;font-size:11pt">&nbsp;Retries</p>';
                var table = false;
                
                var tblData = Array();
                var rsJobs = jsonDataSet.rsJobs;
                
                for (var i=0;i<rsJobs.length;i++) {
                    tblData.push([rsJobs[i].SchedulerJobId, rsJobs[i].EventRange, '<span title="'+rsJobs[i].AppStatusReason+'">'+rsJobs[i].JobExitCode+'<span>', rsJobs[i].PandaExitStatus, '<span title="'+rsJobs[i].ErrorReason+'">'+rsJobs[i].PandaErrorCategory+'<span>', rsJobs[i].Site, rsJobs[i].submitted, rsJobs[i].started, rsJobs[i].finished]);
                }
                
                table = {
                    'tblLabels':['SchedulerJobId','Id in Task','Appl Exit Code','Panda Exit Status','Panda Error Category','Site','Submitted','Started','Finished'],
                    'tblData':tblData
                };
                
                return [['properties',properties],['table',table],['html',html]];
            }
        },
        'sorting':[1,'desc'], // [<column_index>,<sorting_direction>], sorting_direction='desc'||'asc'
        'iDisplayLength': 25, // Number of rows to display on single page
        // Column labels
        'tblLabels': ['SchedulerJobId','Job Name','Job Status'/*,'Appl Exit Code','Grid End Status'*/,'Retries','Site','Submitted','Started','Finished'],
        // dataTables plugin column options
        // see: http://www.datatables.net/usage/columns
        'aoColumns': [
            {"sWidth":"200px"},
            {"sClass":"numericTD","sWidth":"200px"},
            {"sWidth":"100px"},
            //{"sClass":"numericTD","sWidth":"110px"},
            //{"sWidth":"110px"},
            {"sClass":"numericTD","sWidth":"70px"},
            null,
            null,
            null,
            null
        ],
        // Function: extracting array of data form Ajax response
        // Example:
        // - Ajax response: {'user_taskstable':[{col_val1, col_val2, ...}, ...]}
        // - Required function: function(data) { return data.user_taskstable; }
        'getDataArray': function(data) {
            //$.bbq.pushState({'user':data.taskjobs[1].username},0);
            return data.taskjobs[0];
        },
        // Function, translates ajax response onto dataTables plugin data format
        // Output: [[col_val1, col_val2, ...], ...]
        'translateData': function(dataJSON) {
            var jobStatuses = {
                'PR':'NotCompleted',
                'P':'Pending',
                'R':'Running',
                'U':'Unknown',
                'S':'Successfull',
                'F':'Failed'
            };
            var gridEndStatus = {
                'D':'Done',
                'A':'Aborted',
                'C':'Cancelled'
            };
            var replaceLinuxEpoch = function(time) {
                if (!time || (time == '1970-01-01 01:01:01')) return 'Unknown';
                else return time;
            };
            var tasksArr = Array();
            for (i in dataJSON) {
                tasksArr.push(Array(
                    ('<div class="wrapColumn" style="width:200px" title="'+dataJSON[i].SchedulerJobId+'"><a target="_blanc" href="http://panda.cern.ch:25980/server/pandamon/query?job='+dataJSON[i].EventRange+'">'+dataJSON[i].SchedulerJobId+'</a></div>' || 'default'),
                    ('<div class="wrapColumn" style="width:200px" title="'+dataJSON[i].EventRange+'">'+dataJSON[i].EventRange+'</div>' || '0'),
                    (jobStatuses[dataJSON[i].STATUS] ? '<div class="status '+jobStatuses[dataJSON[i].STATUS]+'">'+jobStatuses[dataJSON[i].STATUS]+'</div>' : '<div class="status Unknown">Unknown</div>'),
                    //(dataJSON[i].JobExecExitCode || 'Not yet'),
                    //(dataJSON[i].GridEndId || 'Unknown'),
                    (dataJSON[i].resubmissions || '0'),
                    ('<a href="http://dashb-atlas-ssb.cern.ch/dashboard/request.py/sitehistory?site='+dataJSON[i].Site+'">'+dataJSON[i].Site+'</a>' || '0'),
                    replaceLinuxEpoch(dataJSON[i].submitted),
                    replaceLinuxEpoch(dataJSON[i].started),
                    replaceLinuxEpoch(dataJSON[i].finished)
                ));
            }
            return tasksArr;
        },
        'charts': [
            {
                'name':'Status Overview',
                'type':'hchart', // (gchart|hchart)
                'onDemand':false,
                // translates data onto requires format:
                // {"chd":"t:60,40","chl":"Hello|World"}
                'translateData':function(dataMem) {
                    var data = dataMem.subs.data;
                    var dataLen = data.length;
                    
                    if (dataLen > 0) {
                        var obj = {
                            'statuses':['S','F','R','PR','P','U'],
                            'statusesCnt':[0,0,0,0,0,0],
                            'statusesLbl':['Successful','Failed','Running','NotCompleted','Pending','Unknown'],
                            'statusesColors':['#59D118','#C50000','#3072F3','#BB72F3','#FF9900','#C2BDDD']
                        };
                        
                        for (var i=0; i<dataLen; i++) {
                            var row = data[i];
                            for (var j=0; j<obj.statuses.length; j++) {
                                if (row.STATUS == obj.statuses[j]) obj.statusesCnt[j]++;
                            }
                        }
                        
                        var data = [];
                        
                        for (var i=0;i<obj.statusesCnt.length;i++) {
                            if (obj.statusesCnt[i] > 0) {
                                if (obj.statuses[i] == 'F') data.push({name:obj.statusesLbl[i]+' ('+obj.statusesCnt[i]+')',color:obj.statusesColors[i],y:obj.statusesCnt[i],sliced:true,selected:true});
                                else data.push({name:obj.statusesLbl[i]+' ('+obj.statusesCnt[i]+')',color:obj.statusesColors[i],y:obj.statusesCnt[i]});
                            }
                        }
                    } else return false;
                    
                    output = {
                        chart: {
                            height:350,
                            width:550,
                            backgroundColor:'#ffffff',
                            borderColor:'#aaaaaa',
                            borderWidth:1
                        },
                        title: {
                            text: 'Status Overview'
                        },
                        tooltip: {
                            formatter: function() {return '<b>'+ this.point.name +'</b>: '+ this.y;}
                        },
                        plotOptions: {
                            pie: {
                                allowPointSelect: true,
                                cursor: 'pointer'
                            }
                        },
                        series: [{
                            type: 'pie',
                            data: data
                        }]
                    };
                    
                    return output;
                }
            },
            {
                'name':'Jobs Distributed by Site',
                'type':'hchart', // (gchart|hchart)
                'onDemand':false,
                // translates data onto requires format:
                // {"chd":"t:60,40","chl":"Hello|World"}
                'translateData':function(dataMem) {
                    var data = dataMem.subs.data;
                    var dataLen = data.length;
                    
                    if (dataLen > 0) {
                        var in_array = function(array, p_val) {
	                        for(var i = 0, l = array.length; i < l; i++) {
		                        if(array[i] == p_val) {
			                        return true;
		                        }
	                        }
	                        return false;
                        }
                        
                        var obj = {
                            'statuses':['S','F','R','PR','P','U'],
                            'statusesCnt':[[],[],[],[],[],[]],
                            'sites':[],
                            'maxJobs':0,
                            'chds':[],
                            'statusesLbl':['Successful','Failed','Running','Not Compl.','Pending','Unknown'],
                            'statusesColors':['#59D118','#C50000','#3072F3','#BB72F3','#FF9900','#C2BDDD']
                        };
                        
                        for (var i=0; i<dataLen; i++) {
                            var row = data[i];
                            if (!in_array(obj.sites, row.Site)) { 
                                obj.sites.push(row.Site);
                                for (var k=0; k<obj.statuses.length; k++) {
                                    obj.statusesCnt[k].push(0);
                                }
                            }
                        }
                        
                        for (var j=0;j<obj.sites.length;j++) {
                            for (var i=0; i<dataLen; i++) {
                                var row = data[i];
                                //alert(obj.sites[j]+'=='+row.Site);
                                if (obj.sites[j] == row.Site) {
                                    for (var k=0; k<obj.statuses.length; k++) {
                                        if (row.STATUS == obj.statuses[k]) obj.statusesCnt[k][j]++;
                                    }
                                }
                            }
                        }
                        
                        var series = [];
                        
                        for (var i=0;i<obj.statuses.length;i++) {
                            var data = [];
                            var showFlag = false;
                            for (var j=0;j<obj.sites.length;j++) {
                                data.push(obj.statusesCnt[i][j]);
                                if (obj.statusesCnt[i][j] > 0) showFlag = true;
                            }
                            if (showFlag) series.push({name:obj.statusesLbl[i],color:obj.statusesColors[i],data:data});
                        }
                    } else return false;
                    
                    var output = {
                        chart: {
                            defaultSeriesType: 'column',
                            height:350,
                            width:550,
                            backgroundColor:'#ffffff',
                            borderColor:'#aaaaaa',
                            borderWidth:1
                        },
                        title: {
                            text: 'Jobs Distributed by Site'
                        },
                        xAxis: {
                            categories: obj.sites,
                            labels: {
                                rotation: -35,
                                align: 'right',
                                style: {
                                    font: "normal 9px 'Lucida Grande', 'Lucida Sans Unicode', Verdana, Arial, Helvetica, sans-serif"
                                }
                            }
                        },
                        yAxis: {
                            min: 0,
                            allowDecimals: false,
                            gridLineWidth: 1,
                            title: {
                                text: 'Number of Jobs'
                            }
                        },
                        tooltip: {
                            shared: true,
                            crosshairs: true
                        },
                        plotOptions: {
                            column: {
                                stacking: 'normal',
                                pointPadding:0,
                                groupPadding:0,
                            }
                        },
                        series: series
                    }
                    return output;
                }
            },
            {
                'name':'Processed Events',
                'type':'hchart', // (gchart|hchart)
                'onDemand':true,
                'dataURL': '/dashboard/request.py/proceventscumulativeAlt',
                'dataURL_params': function(Data) { return {'taskmonid':Data.tid}; },
                // translates data onto requires format:
                // {"chd":"t:60,40","chl":"Hello|World"}
                'translateData':function(jsonDataSet) {
                    var procEventsArr = jsonDataSet.allfinished[0];
                    
                    if (procEventsArr.length == 0) return false;
                    
                    var statusesArr = ['FINISHED','FAILED'];
                    var series = [
                        {
                            name:'Finished',
                            color:'#32CD32',
                            data:[]
                        }
                    ];
                    var startdatestring = jsonDataSet.firststarted[0][0]['started'];
		            var enddatestring = jsonDataSet.lastfinished[0][0]['finished'];
		            var timeInterval = (enddatestring - startdatestring)/60;
		            var succjobstotalevents = jsonDataSet.succjobs[0][0]['TOTALEVENTS'];
		            
		            //procEventsArr.sort(function(a,b){return (Date.parse(getDateObj(a.finished))-Date.parse(b.finished));});
                    var bufF = 0;
                    for (var i=0;i<procEventsArr.length;i++) {
                        bufF += procEventsArr[i].Events;
                        var x = procEventsArr[i].finished*1000;
                        var obj = {y:bufF,x:x};
                        //alert('{y:'+bufF+',x:'+x+'}');
                        series[0].data.push(obj);
                    }
                    
                    if (procEventsArr.length == 1) {
                        var obj = {y:0,x:(startdatestring*1000)};
                        series[0].data.splice(0,0,obj);
                    }
                    
                    options = {
                        chart: {
                            defaultSeriesType: 'area',
                            height:350,
                            width:550,
                            backgroundColor:'#ffffff',
                            borderColor:'#aaaaaa',
                            borderWidth:1
                        },
                        title: {
                            text: succjobstotalevents+' processed events in '+Math.ceil(timeInterval)+' minutes'//'Job Evolution Cumulative Plot'
                        },
                        xAxis: {
                            type: "datetime",
                            startOnTick: false,
                            endOnTick: false,
                            showLastLabel: true,
                            gridLineWidth: 1,
                            //tickInterval:tickInterval,
                            dateTimeLabelFormats: {
                                second: '%H:%M',
                                minute: '%H:%M',
                                hour: '%H:%M',
                                day: '%H:%M' 
                            }
                        },
                        yAxis: {
                            min: 0,
                            allowDecimals: false,
                            gridLineWidth: 1,
                            title: {
                                text: 'Events Number'
                            }
                        },
                        legend: {enabled:false},
                        tooltip: {
                            shared: false,
                            crosshairs: true,
                            formatter: function() {
                                var d = new Date(this.x);
                                return d.toUTCString()+'<br>'+
                                    '<span style="color:'+this.series.color+'">Events:</span> <b>'+ this.y +'</b>';
                            }
                        },
                        plotOptions: {
                            area: {
                                marker: { radius:1 },
                                fillColor: '#dddddd',
                                shadow: false
                            }
                        },
                        series: series
                    }
                    return options;
                }
            }/*,
            {
                'name':'Application Failed Jobs by Reason of Failure',
                'type':'gchart', // (gchart|hchart)
                'onDemand':true,
                'dataURL': '/dashboard/request.py/terminatedjobsappAtl',
                'dataURL_params': function(Data) { return {'taskmonid':Data.tid}; },
                // translates data onto requires format:
                // {"chd":"t:60,40","chl":"Hello|World"}
                'translateData':function(data) {
                    var failArr = data.abortedByApplicationReason[0];
                    var dataLen = failArr.length;
                    
                    var obj = {
                        'statusesCnt':[],
                        'statusesLbl':[]
                    };
                    
                    for (var i=0; i<dataLen; i++) {
                        var row = failArr[i];
                        obj.statusesCnt.push(row.TOTAL);
                        obj.statusesLbl.push(row.JobExitReason+' ('+row.TOTAL+')');
                    }
                    
                    var output = {
                        "chd":"t:"+obj.statusesCnt.join(','),
                        "chl":obj.statusesLbl.join('|'),
                        'chtt':'Application Failed Jobs by Reason of Failure',
                        'cht':'p3',
                        'chs':'430x190'
                    };
                    return output;
                }
            }*/
        ]
    };
    // Task Jobs settings - FINISH
}

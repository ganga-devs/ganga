// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 18.05.2010 Created
//

// Inherits from Events()
Controller.prototype = new Events();

function Controller() {
    // Data class initialization
    this.Settings = new Settings();
    this.Data = new Data($('#ajaxAnimation'), this.Settings.Application.modelDefaults, this.Settings.Application.jsonp);
   
    this.mainsTable = Array();
    this.subsTable = Array();
   
    // "viewUpdater" function updates all page controls
    // and decides what to display based on available data
    this.viewUpdater = function() {
        var _Settings = this.Settings.Application; // Shortcut
       
        if (this.Data.user || !_Settings.userSelection) {
            if (this.Data.tid) {
                // Show subs
                // Charts tab chandling - start
                $("#siteTabs").tabs("enable",1); // Enable charts tab
                $("#siteTabs").tabs("select",0); // Select data table tab
                // Charts tab chandling - finish
                this.mainsTable = Array();
                this.drawSubsTable();
            }
            else {
                //show mains
                // Charts tab chandling - start
                $("#siteTabs").tabs("enable",1); // Enable charts tab
                $("#siteTabs").tabs("select",0); // Select data table tab
                // Charts tab chandling - finish
                this.Data.uparam = [];
                this.subsTable = Array();
                this.drawMainsTable();
            }
        }
        else if (_Settings.userSelection) {
            // Show users
            // Charts tab chandling - start
            $('#chartContent').empty(); // Empty charts tab
            $("#siteTabs").tabs("select",0); // Select data table tab
            $("#siteTabs").tabs("disable",1); // Disable charts tab
            // Charts tab chandling - finish
            this.Data.uparam = [];
            this.mainsTable = Array();
            this.subsTable = Array();
            this.drawUsers();
        }
        this.timeRange_update();
        if (_Settings.userSelection) this.userDropdown_update(); // Update only if avaliable
        this.fromTill_update();
        this.userRefresh_update();
        this.breadcrumbs_update();
        this.setupURL();
    };
   
    this.drawSubsTable = function() {
        var thisRef = this;
        var _Settings = this.Settings.Subs; // Shortcut
       
        // "draw" function is calling lkfw.datatable plugin to create table filled with data
        var draw = function(data) {
            thisRef.subsTable = $('#tableContent').lkfw_dataTable({
                dTable: thisRef.subsTable,
                tableId: 'subs',
                expandableRows: _Settings.expandableRows,
                multipleER: _Settings.multipleER,
                items: data,
                tblLabels: _Settings['tblLabels'],
                sorting: (thisRef.Data.sorting.length > 0 ? thisRef.Data.sorting : _Settings.sorting),
                fnERContent:function(dataID){ return thisRef.expand_click(dataID) },
                fnContentChange: function(el) { thisRef.subsTableContent_change(el) },
                fnERClose: function(dataID) { thisRef.erClose_click(dataID) },
                fnTableSorting: function(el) { thisRef.tableSorting_click(el,thisRef.subsTable[0]) },
                dataTable: {
                    iDisplayLength: _Settings.iDisplayLength,
                    sPaginationType: "input",
                    bLengthChange: false,
                    aoColumns: _Settings.aoColumns
                }
            });
        };
       
        // "getData" function converts data given by ajax request onto
        // lkfw.datatable plugin readable format
        // Arguments:
        //   data - object returned by ajax call
        var getData = function(data) {
            var mainSubs = _Settings.getDataArray(data);
            var t = new Date();
           
            // Save the data
            thisRef.Data.mem.subs = {
                user: this.Data.user,
                timestamp: Math.floor(t.getTime()/1000),
                data: mainSubs
            };
           
            // Draw data table
            draw(_Settings.translateData(mainSubs));
           
            tSettings = thisRef.subsTable[0].fnSettings();
            tPages = parseInt( (tSettings.fnRecordsDisplay()-1) / tSettings._iDisplayLength, 10 ) + 1;
           
            if ( $.bbq.getState('p') && ($.bbq.getState('p') <= tPages) ) {
                $('#url-page').trigger('click');  // Load page number from URL
            } else {
                thisRef.Data.p = 1;
                thisRef.Data.noreload = true;
                thisRef.setupURL();
            }
           
            $.each(thisRef.Data.or, function() {
                $('#tablePlus_'+this).parent().trigger('click');
            });
           
            thisRef.drawCharts(_Settings.charts);
        };
       
        // Get the data from ajax call
        this.Data.ajax_getData('subsReq', _Settings.dataURL, _Settings.dataURL_params(this.Data), getData, function(){});
    };
   
    // "drawDataTable" draws data table for subs (in ganga nomenclature)
    // or mains (in CMS nomenclature)
    this.drawMainsTable = function() {
        var thisRef = this;
        var _Settings = this.Settings.Mains; // Shortcut
       
        // "draw" function is calling lkfw.datatable plugin to create table filled with data
        var draw = function(data) {
            thisRef.mainsTable = $('#tableContent').lkfw_dataTable({
                dTable: thisRef.mainsTable,
                tableId: 'mains',
                expandableRows: _Settings.expandableRows,
                multipleER: _Settings.multipleER,
                items: data,
                tblLabels: _Settings['tblLabels'],
                rowsToExpand: thisRef.Data.or,
                sorting: (thisRef.Data.sorting.length > 0 ? thisRef.Data.sorting : _Settings.sorting),
                fnERContent:function(dataID){ return thisRef.expand_click(dataID) },
                fnContentChange: function(el) { thisRef.mainsTableContent_change(el) },
                fnERClose: function(dataID) { thisRef.erClose_click(dataID) },
                fnTableSorting: function(el) { thisRef.tableSorting_click(el,thisRef.mainsTable[0]) },
                dataTable: {
                    iDisplayLength: _Settings.iDisplayLength,
                    sPaginationType: "input",
                    bLengthChange: false,
                    aoColumns: _Settings.aoColumns
                }
            });
        };
       
        // "getData" function converts data given by ajax request onto
        // lkfw.datatable plugin readable format
        // Arguments:
        //   data - object returned by ajax call
        var getData = function(data) {
            var userMains = _Settings.getDataArray(data);
            var t = new Date();
            var tSettings, tPages;
           
            // Save the data
            thisRef.Data.mem.mains = {
                user: this.Data.user,
                timestamp: Math.floor(t.getTime()/1000),
                data: userMains
            };
           
            // Draw data table
            draw(_Settings.translateData(userMains));
           
            // Setting up current page - START
            tSettings = thisRef.mainsTable[0].fnSettings();
            tPages = parseInt( (tSettings.fnRecordsDisplay()-1) / tSettings._iDisplayLength, 10 ) + 1;
           
                    if ( $.bbq.getState('p') && ($.bbq.getState('p') <= tPages) ) {
                $('#url-page').trigger('click');  // Load page number from URL
                thisRef.Data.noreload = true;  // tell keyup event that page hes been reloaded (history is not working without this)
                $('#dataTable_0_paginate input').trigger('keyup');  // Recreate expand events for current page
                thisRef.Data.noreload = false;  // Make sure that noreload is off after operation
            }
            else {
                thisRef.Data.p = 1;
                $('#dataTable_0_paginate input').trigger('keyup');  // Recreate expand events for current page
                thisRef.Data.noreload = true;
                thisRef.setupURL();
            }
            // Setting up current page - FINISH
           
            $.each(thisRef.Data.or, function() {
                $('#tablePlus_'+this).parent().trigger('click');
            });
           
            thisRef.drawCharts(_Settings.charts);
        };
       
        // Get the data from ajax call
        this.Data.ajax_getData('mainsReq', _Settings.dataURL, _Settings.dataURL_params(this.Data), getData, function(){});
    };
   
    this.drawCharts = function(_charts) {
        var thisRef = this;
        var cnt = 1;
       
        var draw = function(gData) {
            var query = gData.join('&');
            thisRef.charts_load(query, cnt);cnt++;
        };
       
        var getData = function(data, chart) {
            var translatedData = chart.translateData(data);
            var gData = [];
            for (key in chart.gChart) { // Adding static variables
                gData.push(key+'='+chart.gChart[key]);
            }
            for (key in translatedData) { // Adding dynamic variables
                gData.push(key+'='+translatedData[key]);
            }
            draw(gData);
        };
       
        $('#chartContent').empty();
       
        try {
            this.charts_prepTable(_charts.length);
            for (var i=0;i<_charts.length;i++) {
                // Get the data from ajax call
                if (_charts[i].dataURL) {
                    this.Data.ajax_getData_sync('chartData_'+i, _charts[i].dataURL, _charts[i].dataURL_params(this.Data), getData, function(){},_charts[i]);
                }
                else {
                    getData(this.Data.mem, _charts[i]);
                }
            }
        } catch(err) {}
       
    };
   
    // "drawUsers" draws users selection page
    this.drawUsers = function() {
        var thisRef = this;
        var _Settings = this.Settings.Users; // Shortcut
       
        // "draw" function is calling lkfw.searchable.list plugin to create searchable list of users
        var draw = function() {
            $('#tableContent').lkfw_searchableList({
                listId: 'users',
                items: thisRef.Data.mem.users,
                srchFldLbl: _Settings.searchLabel
            });
           
            $('#users_0 li').unbind('click').click( function() { thisRef.userListItem_Click(this) });
            $('#users_0 li').unbind('mouseover').mouseover( function() { thisRef.userListItem_MouseOver(this) });
            $('#users_0 li').unbind('mouseout').mouseout( function() { thisRef.userListItem_MouseOut(this) });
        };
       
        // Draw searchable list
        if (this.Data.mem.users) draw();
    };
   
    // "getUsers" retrieves users list and builds user drop-down selection field
    this.getUsers = function() {
        var thisRef = this;
        var _Settings = this.Settings.Users; // Shortcut
       
        // "getData" function converts data given by ajax request onto
        // lkfw.searchable.list plugin readable format
        // Arguments:
        //   data - object returned by ajax call
        var getData = function(data) {
            thisRef.Data.mem.users = _Settings.translateData(data);
           
            thisRef.generateUserDropdownOptions();
           
            if (!(this.Data.user || $.bbq.getState('user'))) thisRef.drawUsers();
        }
       
        // Get the users list from ajax call
        this.Data.ajax_getData('usersReq', _Settings.dataURL, _Settings.dataURL_params(this.Data), getData, function(){});
    };
   
    // "setupURL" builds url fragmant for bookmarking
    this.setupURL = function() {
        $.bbq.pushState({
            user:this.Data.user,
            from:this.Data.ts2iso(this.Data.from),
            till:this.Data.ts2iso(this.Data.till),
            timeRange:this.Data.timeRange,
            refresh:this.Data.refresh,
            tid:this.Data.tid,
            p:this.Data.p,
            sorting:this.Data.sorting,
            or:this.Data.or,
            uparam:this.Data.uparam
        },2);
    };
   
    // "Init" initializes the monitoring system
    this.Init = function() {
        var _Settings = this.Settings.Application; // Shortcut
        thisRef = this;
       
        // Application settings
        // Remove users drop down box
        if (!_Settings.userSelection) $('#userDropBox').hide();
        $('title').text(_Settings.pageTitle); // Set page title
        $('#footerTxt').text(_Settings.footerTxt); // Set footer text
        $('#supportLnk').attr('href', _Settings.supportLnk);
        $('#logo').css('background-image', 'url('+_Settings.logoLnk+')');
       
        // Events definitions
        $('#timeRange').change( function() { thisRef.timeRange_Change(this) });
        $('#refresh').change( function() { thisRef.refresh_Change(this) });
       
        // Activate datepicker
        $('#from, #till').datepicker({
            dateFormat: 'yy-mm-dd',
                        changeMonth: true,
                        changeYear: true
                }).change( function() { thisRef.fromTill_Change(this) });
               
                // Activate tabs
        $("#siteTabs").tabs();
       
        // Setup Data from URL
        this.Data.quickSetup($.bbq.getState());
       
        // Get and setup users list
        this.getUsers();
       
        // Bind the event onhashchange
        $(window).bind('hashchange', function(){
            thisRef.Data.quickSetup($.bbq.getState());
            if (!thisRef.Data.noreload) thisRef.viewUpdater();
            else thisRef.Data.noreload = false;
        });
       
        this.viewUpdater();
       
        // Set up refresh
        this.refresh_Change('#refresh');
    };
}

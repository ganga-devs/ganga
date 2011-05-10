// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 18.05.2010 Created
// 17.01.2011 First production release (v1.0.0)
// 31.03.2011 Major v1.2.0 release (many changes to settings and core of the application)
//

// Inherits from Events()
Controller.prototype = new Events();

function Controller() {
    // Data class initialization
    this.Settings = new Settings();
    this.Data = new Data($('#ajaxAnimation'), this.Settings);
    
    this.mainsTable = Array();
    this.subsTable = Array();
    
    this.appDisplayState = function() {
        var _Settings = this.Settings.Application; // Shortcut
        if (this.Data.user || !_Settings.userSelection) {
            if (this.Data.tid) {
                // Show subs
                return 'subs';
            }
            else {
                //show mains
                return 'mains';
            }
        }
        else if (_Settings.userSelection) {
            // Show users
            return 'users';
        }
    };
    
    // "viewUpdater" function updates all page controls
    // and decides what to display based on available data
    this.viewUpdater = function() {
        var _Settings = this.Settings.Application; // Shortcut
        
        if (this.appDisplayState() == 'subs') {
            // Show subs
            this.mainsTable = Array();
            this.drawSubsTable();
        }
        else if (this.appDisplayState() == 'mains') {
            //show mains
            this.Data.uparam = [];
            this.subsTable = Array();
            this.drawMainsTable();
        }
        else if (this.appDisplayState() == 'users') {
            // Show users
            this.Data.uparam = [];
            this.mainsTable = Array();
            this.subsTable = Array();
            this.drawUsers();
        }
        this.timeRange_update();
        if (_Settings.userSelection) this.userDropdown_update(); // Update only if avaliable
        this.fromTill_update();
        this.userRefresh_update();
        this.filtersUpdate();
        //this.breadcrumbs_update(); // Now changers after tables are loaded
        this.setupURL();
    };
    
    // "drawUsers" draws users selection page
    this.drawUsers = function() {
        var thisRef = this;
        var _Settings = this.Settings.Users; // Shortcut
        
        // "draw" function is calling lkfw.searchable.list plugin to create searchable list of users
        var draw = function() {
            // Charts tab chandling - start
            $('#chartContent').empty(); // Empty charts tab
            $("#siteTabs").tabs("select",0); // Select data table tab
            $("#siteTabs").tabs("disable",1); // Disable charts tab
            $('#topTableCharts').empty();
            // Charts tab chandling - finish
            
            $('#tableContent').lkfw_searchableList({
                listId: 'users',
                items: thisRef.Data.mem.users,
                srchFldLbl: _Settings.searchLabel
            });
            
            $('#users_0 li').unbind('click').click( function() { thisRef.userListItem_Click(this) });
            $('#users_0 li').unbind('mouseover').mouseover( function() { thisRef.userListItem_MouseOver(this) });
            $('#users_0 li').unbind('mouseout').mouseout( function() { thisRef.userListItem_MouseOut(this) });
            thisRef.breadcrumbs_update();
        };
        
        // Hide filters panel
        $('#dataFilters').hide();
        
        // Draw searchable list
        if (this.Data.mem.users) draw();
        
    };
    
    // "getUsers" retrieves users list and builds user drop-down selection field
    this.getUsers = function() {
        var thisRef = this;
        var _Settings = this.Settings.Users; // Shortcut
        
        if (_Settings.dataURL_params === undefined) _Settings.dataURL_params = function() { return {}; };
        
        // "getData" function converts data given by ajax request onto
        // lkfw.searchable.list plugin readable format
        // Arguments:
        //   data - object returned by ajax call
        var getData = function(data) {
            try {
                thisRef.Data.mem.users = _Settings.translateData(data);
            } catch(err) {
                if (thisRef.Settings.Application.debugMode) thisRef.setupErrorDialog(err);
            }
            
            thisRef.generateUserDropdownOptions();
            
            if (!(this.Data.user || $.bbq.getState('user'))) thisRef.drawUsers();
        }
        
        // Get the users list from ajax call
        this.Data.ajax_getData('usersReq', _Settings.dataURL, _Settings.dataURL_params(this.Data), getData, function(){});
    };
    
    // "drawDataTable" draws data table for subs (in ganga nomenclature)
    // or mains (in CMS nomenclature)
    this.drawMainsTable = function() {
        var thisRef = this;
        var _Settings = this.Settings.Mains; // Shortcut
        
        if (_Settings.dataURL_params === undefined) _Settings.dataURL_params = function() { return {}; };
        
        // "draw" function is calling lkfw.datatable plugin to create table filled with data
        var draw = function(data) {
            // Charts tab chandling - start
            if (_Settings.charts !== undefined) $("#siteTabs").tabs("enable",1); // Enable charts tab
            else $("#siteTabs").tabs("disable",1); // Disable charts tab
            $("#siteTabs").tabs("select",0); // Select data table tab
            $('#topTableCharts').empty();
            // Charts tab chandling - finish
            
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
                fnERContentPostProcess:function(expandedID,inputObj){ return thisRef.expand_click_postprocess(expandedID,inputObj,true) },
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
            thisRef.breadcrumbs_update();
            
            // Create filters elements
            thisRef.drawFilters();
            
            $('#loadingTable').stop(true, true).fadeOut(400);
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
            try {
                draw(_Settings.translateData(userMains));
            } catch(err) {
                if (thisRef.Settings.Application.debugMode) thisRef.setupErrorDialog(err);
            }
            
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
            
            // Hide filters panel
            if (_Settings.filters !== undefined) $('#dataFilters').show();
            else $('#dataFilters').hide();
            
            thisRef.executeCharts(_Settings.charts, 'cht_', '#chartContent');
            thisRef.executeCharts(_Settings.topTableCharts, 'topTblcht_', '#topTableCharts', true);
        };
        
        // Get the data from ajax call
        $('#loadingTable').delay(800).fadeIn(400); // Dim content area
        $('#breadcrumbs a').css('color','#888888').unbind();
        this.Data.ajax_getData('mainsReq', _Settings.dataURL, _Settings.dataURL_params(this.Data), getData, function(){});
    };
    
    this.drawSubsTable = function() {
        var thisRef = this;
        var _Settings = this.Settings.Subs; // Shortcut
        
        if (_Settings.dataURL_params === undefined) _Settings.dataURL_params = function() { return {}; };
        
        // "draw" function is calling lkfw.datatable plugin to create table filled with data
        var draw = function(data) {
            // Charts tab handling - start
            if (_Settings.charts !== undefined) $("#siteTabs").tabs("enable",1); // Enable charts tab
            else $("#siteTabs").tabs("disable",1); // Disable charts tab
            $("#siteTabs").tabs("select",0); // Select data table tab
            $('#topTableCharts').empty();
            // Charts tab handling - finish
            
            thisRef.subsTable = $('#tableContent').lkfw_dataTable({
                dTable: thisRef.subsTable,
                tableId: 'subs',
                expandableRows: _Settings.expandableRows,
                multipleER: _Settings.multipleER,
                items: data,
                tblLabels: _Settings['tblLabels'],
                sorting: (thisRef.Data.sorting.length > 0 ? thisRef.Data.sorting : _Settings.sorting),
                fnERContent:function(dataID){ return thisRef.expand_click(dataID) },
                fnERContentPostProcess:function(expandedID,inputObj){ return thisRef.expand_click_postprocess(expandedID,inputObj,false) },
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
            thisRef.breadcrumbs_update();
            
            // Create filters elements
            thisRef.drawFilters();
            
            $('#loadingTable').stop(true, true).fadeOut(400);
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
            try {
                draw(_Settings.translateData(mainSubs));
            } catch(err) {
                if (thisRef.Settings.Application.debugMode) thisRef.setupErrorDialog(err);
            }
            
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
            
            // Hide filters panel
            if (_Settings.filters !== undefined) $('#dataFilters').show();
            else $('#dataFilters').hide();
            
            thisRef.executeCharts(_Settings.charts, 'cht_', '#chartContent');
            thisRef.executeCharts(_Settings.topTableCharts, 'topTblcht_', '#topTableCharts', true);
        };
        
        // Get the data from ajax call
        $('#loadingTable').delay(800).fadeIn(400); // Dim content area
        $('#breadcrumbs a').css('color','#888888').unbind();
        this.Data.ajax_getData('subsReq', _Settings.dataURL, _Settings.dataURL_params(this.Data), getData, function(){});
    };
    
    this.drawChart = function(_charts, domIdPrefix, cnt, forceDraw) {
        if (forceDraw === undefined) forceDraw = false;
        if (_charts[cnt].onDemand === undefined) _charts[cnt]['onDemand'] = false;
        
        var thisRef = this;
        
        var gChartDraw = function(gData) {
            var query = gData.join('&');
            thisRef.charts_load(query, domIdPrefix, cnt);
        };
        
        var hChartDraw = function(hData) {
            if (hData.chart === undefined) hData['chart'] = {};
            hData.chart['renderTo'] = domIdPrefix+cnt;
            hData['credits'] = false;
            new Highcharts.Chart(hData);
        };
        
        var getData = function(data, chart) {
            var translatedData = chart.translateData(data);
            if (translatedData == false) {
                thisRef.drawNoDataMessage(_charts, domIdPrefix, cnt);
                return false;
            }
            if (chart.type == 'gchart') {
                var gData = [];
                for (key in translatedData) { // Adding dynamic variables
                    gData.push(key+'='+translatedData[key]);
                }
                gChartDraw(gData);
            }
            else if (chart.type == 'hchart') {
                hData = chart.translateData(data);
                hChartDraw(hData);
            }
        };
        
        if (_charts[cnt].onDemand == false || forceDraw == true) {
            // Get the data from ajax call
            if (_charts[cnt].dataURL) {
                if (_charts[cnt].dataURL_params === undefined) _charts[cnt].dataURL_params = function() { return {}; };
                this.Data.ajax_getData_sync('chartData_'+i, _charts[cnt].dataURL, _charts[cnt].dataURL_params(this.Data), getData, function(){},_charts[cnt]);
            }
            else {
                getData(this.Data.mem, _charts[cnt]);
            }
        }
        else {
            this.drawChtRequestButton(_charts, domIdPrefix, cnt);
        }
    };
    
    this.executeCharts = function(_charts, domIdPrefix, tableTarget, hideTargetElement) {
        var thisRef = this;
        
        try {
            $(tableTarget).empty();
            this.charts_prepTable(_charts.length, tableTarget, domIdPrefix);
            for (var cnt=0;cnt<_charts.length;cnt++) {
                this.drawChart(_charts, domIdPrefix, cnt);
            }
        } catch(err) {
            if (_charts !== undefined && this.Settings.Application.debugMode) this.setupErrorDialog(err);
        }
    };
    
    // "setupURL" builds url fragmant for bookmarking
    this.setupURL = function() {
        var thisRef = this;
            
        var updateHashwithFilters = function(urlHash, _Settings) {
            if (_Settings.filters !== undefined) {
                for (var i=0;i<_Settings.filters.length;i++) {
                    urlHash[_Settings.filters[i].urlVariable] = thisRef.Data.filters[_Settings.filters[i].urlVariable];
                }
            }
            return urlHash;
        };
        
        var urlHash = {
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
        };
        
        if (this.appDisplayState() == 'mains') {
            urlHash = updateHashwithFilters(urlHash, this.Settings.Mains);
        }
        else if (this.appDisplayState() == 'subs') {
            urlHash = updateHashwithFilters(urlHash, this.Settings.Subs);
        }
        $.bbq.pushState(urlHash,2);
    };
    
    // "Init" initializes the monitoring system
    this.Init = function() {
        var _Settings = this.Settings.Application; // Shortcut
        thisRef = this;
        
        // Application settings
        // Remove users drop down box
        if (!_Settings.userSelection && _Settings.userSelection !== undefined) $('#userDropBox').hide();
        if (!_Settings.dataRefresh && _Settings.dataRefresh !== undefined) $('#refreshDropBox').hide();
        if (!_Settings.timeRangeSelection && _Settings.timeRangeSelection !== undefined) $('#timeSelect').hide();
        $('title').text(_Settings.pageTitle); // Set page title
        $('#footerTxt').html(_Settings.footerTxt); // Set footer text
        $('#supportLnk').attr('href', _Settings.supportLnk);
        $('#logo').css('background-image', 'url('+_Settings.logoLnk+')');
        $("#dialog-message").dialog({autoOpen: false});
        
        // Events definitions
        $('#timeRange').change( function() { thisRef.timeRange_Change(this) });
        $('#refresh').change( function() { thisRef.refresh_Change(this) });
        $('#refreshImg').click( function() { thisRef.viewUpdater() } );
        
        // Activate datepicker
        $('#from, #till').datepicker({
            dateFormat: 'yy-mm-dd',
			changeMonth: true,
			changeYear: true
		}).change( function() { thisRef.fromTill_Change(this) });
		
		// Activate tabs
        $("#siteTabs").tabs({select: function(event, ui) {
            if (ui.index == 1) $('#topTableCharts').hide();
            else $('#topTableCharts').show();
        }});
        
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

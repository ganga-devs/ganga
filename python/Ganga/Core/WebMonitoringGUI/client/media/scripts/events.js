// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 18.05.2010 Created
//

Events.prototype = new ControlsUpdate();

function Events() {
    this.userDropDown_Change = function(el) {
        $('.tablePlus').attr('src', 'media/images/table_plus.png');
        //this.Data.or = [];
        this.Data.tid = '';
	this.Data.sorting = [];
        this.Data.user = $(el).val();
        this.setupURL();
    };

    this.userListItem_Click = function(el) {
        this.Data.user = $(el).text();
        this.setupURL();
    };
    
    this.timeRange_Change = function(el) {
        this.Data.timeRange = $(el).val();
        this.Data.from = 0;
        this.Data.till = 0;
        $('.tablePlus').attr('src', 'media/images/table_plus.png');
        //this.Data.or = [];
        this.setupURL();
    };
    
    this.refresh_Change = function(el) {
        var thisRef = this;
        this.Data.refresh = parseInt($(el).val());
        
        try { clearInterval(this.intervalID); } finally {}
        if (this.Data.refresh > 0) this.intervalID = setInterval( function() {thisRef.viewUpdater()}, (this.Data.refresh*1000));
        //this.Data.noreload = true;
        //$('.tablePlus').attr('src', 'media/images/table_plus.png');
        //this.Data.or = [];
        this.setupURL();
    };
    
    this.fromTill_Change = function(el) {
        if( !this.Data.changeFromTill(el.id, ($.datepicker.formatDate('@',$(el).datepicker( "getDate" )))) ) {
            if (this.Data[el.id] == 0) $(el).datepicker( "setDate", null );
            else $(el).datepicker( "setDate", $.datepicker.parseDate('@',(this.Data[el.id])) );
        }
        this.Data.timeRange = '';
        $('.tablePlus').attr('src', 'media/images/table_plus.png');
        this.Data.or = [];
        this.setupURL();
    };
    
    this.userListItem_MouseOver = function(el) {
        $(el).css('background-color','#d7e6f1');
    };
    
    this.userListItem_MouseOut = function(el) {
        $(el).css('background-color','#ffffff');
    };
    
    this.breadcrumbs_click = function(el) {
        if ($(el).text() == 'Users List') {
            this.Data.tid = '';
            this.Data.sorting = [];
            this.Data.uparam = [];
            this.Data.user = '';
        }
        else if ($(el).text() == 'Jobs') {
            this.Data.tid = '';
            this.Data.sorting = [];
            this.Data.uparam = [];
        }
        $('.tablePlus').attr('src', 'media/images/table_plus.png');
        this.Data.or = [];
        this.setupURL();
    };
    
    this.taskExpand_click = function(dataID) {
        var _Settings = this.Settings.Mains; // Shortcut
        var dataSet = this.Data.mem.tasks.data[dataID];
        
        if ($.inArray(dataID, this.Data.or) == -1) {
            if (_Settings.multipleER) this.Data.or.push(dataID);
            else this.Data.or = [dataID];
            this.Data.noreload = true;
            this.setupURL();
        }
        
        return _Settings.expandData(dataSet);
    };
    
    this.taskClose_click = function(dataID) {
        var _Settings = this.Settings.Mains; // Shortcut
        if (_Settings.multipleER) {
            var position = $.inArray(dataID, this.Data.or);
            this.Data.or.splice(position,1);
        }
        else this.Data.or = [];
        this.Data.noreload = true;
        this.setupURL();
    };
    
    this.tasksTableContent_change = function(el) {
        var thisRef = this;
        this.Data.p = $('#dataTable_0_paginate input').val();
        if (this.Data.noreload == false) {
            $('.tablePlus').attr('src', 'media/images/table_plus.png');
            this.Data.or = [];
        }
        $('#dataTable_0 tbody a').parents('td').unbind();
        $('#dataTable_0 tbody a').parents('td').click(function(){ thisRef.gotoTask_click(this); });
        this.Data.noreload = true;
        this.setupURL();
    };
    
    this.gotoTask_click = function(el) {
        var _Settings = this.Settings.Mains; // Shortcut
        var aPos = this.tasksTable[0].fnGetPosition(el);
	//12th column is the monitoring link - in that case we want to open the link, not to go to subjobs
        if (aPos[1] != 12)
	{
        	_Settings.setupUserParams(this.Data, el, aPos);
        	this.Data.or = [];
		this.Data.sorting = [];
        	this.Data.p = 1;
        	this.Data.noreload = false;
        	this.setupURL();
	}
    };
    
    this.jobsTableContent_change = function(el) {
        var thisRef = this;
        this.Data.p = $('#dataTable_0_paginate input').val();
        this.Data.noreload = true;
        this.setupURL();
    };


    this.tableSorting_click = function(el, dataTable) {
        tSettings = dataTable.fnSettings();
        this.Data.sorting = Array(tSettings.aaSorting[0][0],tSettings.aaSorting[0][1]);
    };
}

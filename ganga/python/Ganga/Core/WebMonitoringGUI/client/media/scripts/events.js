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
        this.Data.noreload = false;
        this.setupURL();
    };

    this.userListItem_Click = function(el) {
        this.Data.user = $(el).text();
        this.Data.noreload = false;
        this.setupURL();
    };
   
    this.timeRange_Change = function(el) {
        this.Data.timeRange = $(el).val();
        this.Data.from = 0;
        this.Data.till = 0;
        $('.tablePlus').attr('src', 'media/images/table_plus.png');
        //this.Data.or = [];
        this.Data.noreload = false;
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
        this.Data.noreload = false;
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
        this.Data.noreload = false;
        this.setupURL();
    };
   
    this.userListItem_MouseOver = function(el) {
        $(el).css('background-color','#d7e6f1');
    };
   
    this.userListItem_MouseOut = function(el) {
        $(el).css('background-color','#ffffff');
    };
   
    this.breadcrumbs_click = function(el) {
        var _Settings = this.Settings.Application; // Shortcut
        if ($(el).text() == _Settings.usersListLbl) {
            this.Data.tid = '';
            this.Data.sorting = [];
            this.Data.uparam = [];
            this.Data.user = '';
        }
        else if ($(el).text() == _Settings.mainsLbl) {
            this.Data.tid = '';
            this.Data.sorting = [];
            this.Data.uparam = [];
        }
        $('.tablePlus').attr('src', 'media/images/table_plus.png');
        this.Data.or = [];
        this.Data.noreload = false;
        this.setupURL();
    };
   
    this.expand_click = function(dataID) {
        if (this.Data.tid == '') {
            var _Settings = this.Settings.Mains; // Shortcut
            var rowDataSet = this.Data.mem.mains.data[dataID];
        }
        else {
            var _Settings = this.Settings.Subs; // Shortcut
            var rowDataSet = this.Data.mem.subs.data[dataID];
        }
       
        var output;
       
        var processData = function(jsonDataSet) {
            if (jsonDataSet === undefined) jsonDataSet = false;
           
            output = _Settings.expandData.dataFunction(rowDataSet, jsonDataSet);
        };
       
        if (_Settings.expandData.dataURL) {
            this.Data.ajax_getData_sync('expand', _Settings.expandData.dataURL, _Settings.expandData.dataURL_params(this.Data, rowDataSet), processData, function(){});
        } else {
            processData();
        }
       
        if ($.inArray(dataID, this.Data.or) == -1) {
            if (_Settings.multipleER) this.Data.or.push(dataID);
            else this.Data.or = [dataID];
            this.Data.noreload = true;
            this.setupURL();
        }
       
        return output;
    };
   
    this.erClose_click = function(dataID) {
        if (this.Data.tid == '') var _Settings = this.Settings.Mains; // Shortcut
        else var _Settings = this.Settings.Subs; // Shortcut
       
        if (_Settings.multipleER) {
            var position = $.inArray(dataID, this.Data.or);
            this.Data.or.splice(position,1);
        }
        else this.Data.or = [];
        this.Data.noreload = true;
        this.setupURL();
    };
   
    this.mainsTableContent_change = function(el) {
        var thisRef = this;
        this.Data.p = $('#dataTable_0_paginate input').val();
        if (this.Data.noreload == false) {
            $('.tablePlus').attr('src', 'media/images/table_plus.png');
            this.Data.or = [];
        }
        $('#dataTable_0 tbody a').parents('td').unbind();
        $('#dataTable_0 tbody a').parents('td').click(function(){ thisRef.gotoMain_click(this); });
        this.Data.noreload = true;
        this.setupURL();
    };
   
    this.gotoMain_click = function(el) {
        var _Settings = this.Settings.Mains; // Shortcut
        var aPos = this.mainsTable[0].fnGetPosition(el);
       
        // setup model
        _Settings.setupUserParams(this.Data, el, aPos);
        this.Data.or = [];
        this.Data.sorting = [];
        this.Data.p = 1;
        this.Data.noreload = false;
        this.setupURL();
    };
   
    this.subsTableContent_change = function(el) {
        var thisRef = this;
        this.Data.p = $('#dataTable_0_paginate input').val();
        this.Data.noreload = true;
        this.setupURL();
    };
   
    this.tableSorting_click = function(el, dataTable) {
        tSettings = dataTable.fnSettings();
        //alert(tSettings.aaSorting[0][1]);
        this.Data.sorting = Array(tSettings.aaSorting[0][0],tSettings.aaSorting[0][1]);
        //this.Data.noreload = true;
        //this.setupURL();
    };
}

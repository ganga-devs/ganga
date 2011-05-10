// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 18.05.2010 Created
// 17.01.2011 First production release (v1.0.0)
//

function ControlsUpdate() {
    this.userDropdown_update = function() {
        var thisRef = this;
        $('#userSelect_dropdown option').each( function(i){
            $(this).removeAttr('selected');
            if ($(this).val() == thisRef.Data.user) $(this).attr('selected','selected');
        });
    };
    
    this.generateUserDropdownOptions = function() {
        // Generate users options
        var newOption = $('<option></option>').attr('value','').html('');
        $('#userSelect_dropdown').empty();
        $('#userSelect_dropdown').append(newOption);
        for (i in thisRef.Data.mem.users) {
            newOption = $('<option></option>').attr('value',this.Data.mem.users[i]).html(this.Data.mem.users[i]);
            $('#userSelect_dropdown').append(newOption);
        }
        
        $('#userSelect_dropdown').unbind('change').change( function() { thisRef.userDropDown_Change(this) });
        this.userDropdown_update();
    };
    
    this.fromTill_update = function() {
        if (this.Data.from == 0) $('#from').datepicker('setDate',null);
        else $('#from').datepicker('setDate',$.datepicker.parseDate('@',(this.Data.from)));
        if (this.Data.till == 0) $('#till').datepicker('setDate',null);
        else $('#till').datepicker('setDate',$.datepicker.parseDate('@',(this.Data.till)));
    };
    
    this.timeRange_update = function() {
        var thisRef = this;
        var timestampNow, timestampThen, timeThen;
        var pastArr = {
            'lastDay': (86400*1000),
            'last2Days': (86400*1000*2),
            'last3Days': (86400*1000*3),
            'lastWeek': (86400*1000*7),
            'last2Weeks': (86400*1000*14),
            'lastMonth': (86400*1000*31)
        };
        $('#timeRange option').each( function(i){
            $(this).removeAttr('selected');
            if ($(this).val() == thisRef.Data.timeRange) $(this).attr('selected','selected');
        });
        
        if (thisRef.Data.timeRange) {
            timestampNow = $.datepicker.formatDate('@', new Date());
            timestampThen = $.datepicker.parseDate('@',(timestampNow - pastArr[thisRef.Data.timeRange]));
            timeThen = 'Records from '+$.datepicker.formatDate('yy-mm-dd', timestampThen);
        }
        else {
            timeThen = 'Disabled';
        }
        $('#timeRange').attr('title',timeThen);
    };
    
    this.userRefresh_update = function() {
        var thisRef = this;
        $('#refresh option').each( function(i){
            $(this).removeAttr('selected');
            if ($(this).val() == thisRef.Data.refresh) $(this).attr('selected','selected');
        });
    };
    
    this.breadcrumbs_update = function() {
        var _Settings = this.Settings.Application; // Shortcut
        var thidRef = this;
        var output = '&nbsp;:: ';
        // id=breadcrumbs
        if (this.Data.user || !_Settings.userSelection) {
            if (this.Data.tid) {
                // show subs
                if (_Settings.userSelection) output += '<a>'+_Settings.usersListLbl+'</a> &raquo; <span class="bold">' + this.Data.user + '</span> &raquo; ';
                output += '<a>'+_Settings.mainsLbl+'</a> &raquo; ' + this.Data.tid;
            }
            else {
                // show mains
                if (_Settings.userSelection) output += '<a>'+_Settings.usersListLbl+'</a> &raquo; <span class="bold">' + this.Data.user + '</span> &raquo; ';
                output += _Settings.mainsLbl;
            }
        }
        else {
            // show users
            output += 'Users List';
        }
        
        $('#breadcrumbs').html(output);
        
        // Set up events
        $('#breadcrumbs a').click( function() { thisRef.breadcrumbs_click(this) });
    };
    
    this.charts_prepTable = function(chtCnt) {
        var rowCnt = Math.ceil((chtCnt/2));
        var table = $('<table></table>').attr({
            'id':'chartTbl',
            'cellpadding':'0',
            'cellspacing':'0'
        }).css('width','100%');
        
        var cnt = 1;
        for (var i=0;i<rowCnt;i++) {
            var tr = $('<tr></tr>');
            tr.append($('<td></td>').attr('id','cht_'+cnt).addClass('chartTd'));cnt++;
            tr.append($('<td></td>').attr('id','cht_'+cnt).addClass('chartTd'));cnt++;
            table.append(tr);
        }
        $('#chartContent').append(table);
    };
    
    this.charts_load = function(query, cnt) {
        $('#cht_'+cnt).append(
            $('<img></img>').attr({
                'src':'http://chart.apis.google.com/chart?'+query,
                'class':'chartImg'
            })
        );
    };
    
    this.drawFilters = function() {
        var _Settings = this.Settings.Application;
        
        var thisRef = this;
        var optArr = [];
        
        if (_Settings.filters !== undefined) {
            $('#dataFiltersInputs').empty();
            for (var i=0;i<_Settings.filters.length;i++) {
                var mainSpan = $('<span></span>').addClass('filterItems').html(_Settings.filters[i].label+'<br />');
                
                if (_Settings.filters[i].fieldType == 'text' || _Settings.filters[i].fieldType == 'date' || _Settings.filters[i].fieldType == 'datetime') {
                    var filter = $('<input></input>').attr({
                        'type':'text',
                        'id':_Settings.filters[i].urlVariable,
                        'value':this.Data.filters[_Settings.filters[i].urlVariable]
                    });
                    mainSpan.append(filter);
                } 
                else if (_Settings.filters[i].fieldType == 'select') {
                    var filter = $('<select></select>').attr('id',_Settings.filters[i].urlVariable);
                    
                    if (_Settings.filters[i].options.dataURL !== undefined) {
                        this.Data.ajax_getData_sync('filter', _Settings.filters[i].options.dataURL, {}, function(data){
                            optArr = _Settings.filters[i].options.translateData(data);
                        }, function(){});
                    } else var optArr = _Settings.filters[i].options.translateData(this.Data);
                    
                    if (optArr.length > 0) for (var j=0;j<optArr.length;j++) {
                        var option = $('<option></option>').attr('value',optArr[j][0]).text(optArr[j][1]);
                        if (optArr[j][0] == this.Data.filters[_Settings.filters[i].urlVariable]) option.attr('selected','selected');
                        filter.append(option);
                    }
                    mainSpan.append(filter);
                }
                
                this.filtersSubmit_OnOff(i);
                
                $('#dataFiltersInputs').append(mainSpan);
            }
            
            // Turn on date pickers
            for (var i=0;i<_Settings.filters.length;i++) {
                if (_Settings.filters[i].fieldType == 'date') {
                    $('#'+_Settings.filters[i].urlVariable).datepicker({
                        dateFormat: 'yy-mm-dd',
			            changeMonth: true,
			            changeYear: true
		            });
                }
                else if (_Settings.filters[i].fieldType == 'datetime') {
                    $('#'+_Settings.filters[i].urlVariable).datetimepicker({
                        dateFormat: 'yy-mm-dd',
			            changeMonth: true,
			            changeYear: true
		            });
                }
            }
            
            $('#submitFilters').click(function(){ thisRef.filtersSubmit_click(this); });
            
            $('#dataFilters').show();
        }
    };
    
    this.filtersUpdate = function() {
        var _Settings = this.Settings.Application; // Shortcut
        
        var thisRef = this;
        if (_Settings.filters !== undefined) {
            for (var i=0;i<_Settings.filters.length;i++) {
                if (_Settings.filters[i].fieldType == 'text' || _Settings.filters[i].fieldType == 'date') {
                    $('.filterItems #'+_Settings.filters[i].urlVariable).attr('value', this.Data.filters[_Settings.filters[i].urlVariable]);
                } 
                else if (_Settings.filters[i].fieldType == 'select') {
                    $('.filterItems #'+_Settings.filters[i].urlVariable+' option').each( function(j){
                        $(this).removeAttr('selected');
                        if ($(this).val() == thisRef.Data.filters[_Settings.filters[i].urlVariable]) $(this).attr('selected','selected');
                    });
                }
                
                this.filtersSubmit_OnOff(i);
            }
            
            this.setupURL();
        }
    };
}












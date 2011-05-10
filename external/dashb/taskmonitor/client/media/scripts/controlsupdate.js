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

function ControlsUpdate() {
    this.setupErrorDialog = function(err) {
        var ul = $('<ul></ul>');
        var st = err.stack.split("\n");
        for (var i=0;i<st.length;i++) {
            if (st[i] != '') ul.append($('<li></li>').html(st[i]));
        }
        $('#dialog-content').html(ul);
        $('#dialog-message').dialog({ 
            title: err.name+': '+err.message,
            modal: true,
            width: 700,
			buttons: {
				Ok: function() {
					$( this ).dialog( "close" );
				}
			} 
	    });
        $('#dialog-message').dialog('open');
    };

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
    
    this.charts_prepTable = function(chtCnt, tableTarget, domIdPrefix) {
        if (chtCnt > 0) {
            var rowCnt = Math.ceil((chtCnt/2));
            var table = $('<table></table>').attr({
                'class':'chartTbl',
                'cellpadding':'0',
                'cellspacing':'0'
            }).css('width','100%');
            
            var cnt = 0;
            for (var i=0;i<rowCnt;i++) {
                var tr = $('<tr></tr>');
                tr.append( $('<td></td>').addClass('chartTd').append( $('<span></span>').attr('id',domIdPrefix+cnt).css({'display':'inline-block'}) ) );cnt++;
                tr.append( $('<td></td>').addClass('chartTd').append( $('<span></span>').attr('id',domIdPrefix+cnt).css({'display':'inline-block'}) ) );cnt++;
                table.append(tr);
            }
            
            $(tableTarget).append(table);
        }
    };
    
    this.charts_load = function(query, domIdPrefix, cnt) {
        $('#'+domIdPrefix+cnt).empty();
        $('#'+domIdPrefix+cnt).append(
            $('<img></img>').attr({
                'src':'http://chart.apis.google.com/chart?'+query,
                'class':'chartImg'
            })
        );
    };
    
    this.drawChtMessageFrame = function(content) {
        var frame = $('<div></div>').addClass('chartMessageFrame');
        return frame;
    };
    
    this.drawChtRequestButton = function(_charts, domIdPrefix, cnt) {
        var thisRef = this;
        
        var chtMessageFrame = this.drawChtMessageFrame();
        $('#'+domIdPrefix+cnt).empty();
        chtMessageFrame.append('<span class="chartTitle">'+_charts[cnt].name+'</span><br />');
        chtMessageFrame.append($('<input />').attr({
            'id':'butt_'+domIdPrefix+cnt,
            'type':'button',
            'value':'Load Chart'
        }).click(function(){ thisRef.drawChtRequestButton_click(this, _charts, domIdPrefix, cnt) }));
        $('#'+domIdPrefix+cnt).append(chtMessageFrame);
    };
    
    this.drawNoDataMessage = function(_charts, domIdPrefix, cnt) {
        var chtMessageFrame = this.drawChtMessageFrame();
        
        chtMessageFrame.append('<span class="chartTitle">'+_charts[cnt].name+'</span><br />There is no data<br />to draw a chart');
        $('#'+domIdPrefix+cnt).empty();
        $('#'+domIdPrefix+cnt).append(chtMessageFrame);
    };
    
    this.drawFilters = function() {
        if (this.appDisplayState() == 'mains') var _Settings = this.Settings.Mains; // Shortcut
        else if (this.appDisplayState() == 'subs') var _Settings = this.Settings.Subs; // Shortcut
        
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
                        if (this.Data.mem.filters[_Settings.filters[i].urlVariable] === undefined) {
                            if (_Settings.filters[i].options.dataURL_params === undefined) _Settings.filters[i].options.dataURL_params = function() { return {}; };
                            this.Data.ajax_getData_sync('filter', _Settings.filters[i].options.dataURL, _Settings.filters[i].options.dataURL_params(this.Data), function(data){
                                try {
                                    thisRef.Data.mem.filters[_Settings.filters[i].urlVariable] = data;
                                    optArr = _Settings.filters[i].options.translateData(data);
                                } catch(err) {
                                    if (thisRef.Settings.Application.debugMode) thisRef.setupErrorDialog(err);
                                }
                            }, function(){});
                        } else {
                            try {
                                optArr = _Settings.filters[i].options.translateData(this.Data.mem.filters[_Settings.filters[i].urlVariable]);
                            } catch(err) {
                                if (thisRef.Settings.Application.debugMode) thisRef.setupErrorDialog(err);
                            }
                        }
                    } else {
                        try {
                            optArr = _Settings.filters[i].options.translateData(this.Data);
                        } catch(err) {
                            if (thisRef.Settings.Application.debugMode) thisRef.setupErrorDialog(err);
                        }
                    }
                    if (optArr == false) return false;
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
        if (this.appDisplayState() != 'users') {
            if (this.appDisplayState() == 'mains') var _Settings = this.Settings.Mains; // Shortcut
            else if (this.appDisplayState() == 'subs') var _Settings = this.Settings.Subs; // Shortcut
            
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
        }
    };
}












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

function Data(ajaxAnimation, _Settings, jsonp) {
    var settings = _Settings.Application.modelDefaults();
    var jsonp = _Settings.Application.jsonp
    // general values
    this.user = settings.user;
    this.from = settings.from;
    this.till = settings.till;
    this.timeRange = settings.timeRange;
    this.refresh = settings.refresh;
    this.tid = settings.tid;
    this.p = settings.p;
    this.sorting = settings.sorting;
    this.or = settings.or; // opened table rows
    this.uparam = settings.uparam; // user defined params (for params that cannot be shared between use cases)
    
    this.noreload = false;
    
    this.filters = {};
        
    // Data
    this.mem = {
        users: Array(),
        mains: {
            user: '',
            timestamp: 0,
            data: Array()
        },
        subs: {
            user: '',
            tid: '',
            timestamp: 0,
            data: Array()
        },
        filters:{}
    };
    
    // Copy val instead create reference
    this.copyVal = function(val) {
        return val;
    };
    
    // Setting up user defined filters
    if (_Settings.Mains.filters !== undefined) {
        for (var i=0;i<_Settings.Mains.filters.length;i++) {
            this.filters[_Settings.Mains.filters[i].urlVariable] = this.copyVal(_Settings.Mains.filters[i].value);
        }
    }
    // Setting up user defined filters
    if (_Settings.Subs.filters !== undefined) {
        for (var i=0;i<_Settings.Subs.filters.length;i++) {
            this.filters[_Settings.Subs.filters[i].urlVariable] = this.copyVal(_Settings.Subs.filters[i].value);
        }
    }
    
    // Ajax xmlhttprequest object used to store table data requests handlers
    this.xmlhttprequest = null;
    
    this.quickSetup = function(params, ts2iso) {
        var settings = _Settings.Application.modelDefaults();
        this.user = (params['user'] || settings.user);
        this.from = parseInt(this.iso2ts(params['from']) || settings.from);
        this.till = parseInt(this.iso2ts(params['till'],2) || settings.till);
        this.timeRange = ( (params['timeRange'] == '') ? params['timeRange'] : (params['timeRange'] || settings.timeRange) );
        this.refresh = (params['refresh'] || settings.refresh);
        this.tid = (params['tid'] || settings.tid);
        this.p = (params['p'] || settings.p);
        this.or = (params['or'] || settings.or);
        this.sorting = (params['sorting'] || []);
        this.uparam = (params['uparam'] || settings.uparam);
        
        // make this.or an array of ints
        for (i in this.or) {
            this.or[i] = parseInt(this.or[i]);
        }
        
        // Setting up user defined filters
        if (_Settings.Mains.filters !== undefined && (this.tid == '' || this.user == '')) {
            for (var i=0;i<_Settings.Mains.filters.length;i++) {
                this.filters[_Settings.Mains.filters[i].urlVariable] = (params[_Settings.Mains.filters[i].urlVariable] || this.copyVal(_Settings.Mains.filters[i].value));
            }
        }
        // Setting up user defined filters
        if (_Settings.Subs.filters !== undefined) {
            for (var i=0;i<_Settings.Subs.filters.length;i++) {
                this.filters[_Settings.Subs.filters[i].urlVariable] = (params[_Settings.Subs.filters[i].urlVariable] || this.copyVal(_Settings.Subs.filters[i].value));
            }
        }
    };
    
    this.setOr = function(dataID) {
        if ($.inArray(dataID, this.or) == (-1)) {
            this.or.push(dataID);
            return true;
        }
        else {
            return false;
        }
    };
    
    // Dates handling - Start
    this.iso2ts = function(date, mode) {
        if (typeof mode == 'undefined') mode = 1;
        if (date == 0 || typeof date == 'undefined') return 0;
        else {
            if (mode == 1) return $.datepicker.formatDate('@', $.datepicker.parseDate('yy-mm-dd',date));
            else if (mode == 2) return parseInt($.datepicker.formatDate('@', $.datepicker.parseDate('yy-mm-dd',date))) + 86399000;
            else return 0;
        }
    };
    
    this.ts2iso = function(date, mode) {
        if (typeof mode == 'undefined') mode = 1;
        if (date == 0 || typeof date == 'undefined') return '';
        else {
            if (mode == 1) return $.datepicker.formatDate('yy-mm-dd', $.datepicker.parseDate('@',date));
            else if (mode == 2) return $.datepicker.formatDate('yy-mm-dd', $.datepicker.parseDate('@',date)) + ' 00:00';
            else if (mode == 3) return $.datepicker.formatDate('yy-mm-dd', $.datepicker.parseDate('@',date)) + ' 23:59';
            else return '';
        }
    };
    
    this.changeFromTill = function(which, timestamp) {
        var output = true;
        if (timestamp == '') timestamp = 0;
        else timestamp = parseInt(timestamp);
        
        if (which == 'from') {
            if (timestamp > this.till && timestamp != 0) {
                this.till = (timestamp + 86399000);
            }
            else if (timestamp == 0) {
                timestamp = (this.till - 86399000);
                output = false;
            }
            this.from = timestamp;
        }
        else if (which == 'till') {
            if (((timestamp+86399000) < this.from || this.from == 0) && timestamp != 0) {
                this.from = timestamp;
            }
            else if (timestamp == 0) {
                timestamp = this.till;
                output = false;
            }
            this.till = (timestamp+86399000);
        }
        return output;
    };
    // Dates handling - Finish
    
    this.addPortNumber = function(url, port) {
        url = url.replace('//','^^');
        if (url.search('/') != -1) {
            url = url.replace('/',':'+port+'/');
        } else {
            url = url+':'+port;
        }
        url = url.replace('^^','//');
        return url;
    };
    
    this.requestErrorDialog = function(xhrName, textStatus, errorThrown) {
        if (_Settings.Application.debugMode) {
            var html = '';
            html += 'Please check your settings file for any url misspells and your server.<br />----------<br />';
            html += 'Test Status: '+textStatus+'<br />----------<br />';
            html += 'Error: '+errorThrown;
            $('#dialog-content').html(html);
            $('#dialog-message').dialog({ 
                title: 'Error during ajax request (xhrName: '+xhrName+')',
                modal: true,
                width: 700,
			    buttons: {
				    Ok: function() {
					    $( this ).dialog( "close" );
					    window.history.back();
				    }
			    } 
	        });
            $('#dialog-message').dialog('open');
        }
        else {
            window.history.back();
        }
    };
    
    // Get job subs from server
    this.ajax_getData = function(xhrName, url, params, fSuccess, fFailure) {
        var thisRef = this;
        
        currentUrl = window.location.toString()
        portIndex = currentUrl.indexOf('?port=');
        if (portIndex > -1) {
            
            port = ''           
            isNumber = true;    
            index = portIndex + 6       
        
            while(isNumber){
                char = currentUrl[index];
                if(char == '0' || char == '1' || char =='2' ||
                   char == '3' || char == '4' || char =='5' ||
                   char == '6' || char == '7' || char =='8' || char =='9'){
                    port = port + currentUrl[index];
                    index++;
                }
                else{
                    isNumber = false
                }
                
            }

            url = this.addPortNumber(url, port);            
        }

        
        var paramsString = '';
        for (x in params) { paramsString += x+'='+params[x]+'&' }
        var key = $.base64Encode(xhrName+'^'+url+'#'+paramsString);
        
        var data = _Cache.get(key);
        if (data) {
            fSuccess(data);
        } else if (url) {
            ajaxAnimation.addClass(xhrName).fadeIn(200);
            if (this.xmlhttprequest) this.xmlhttprequest.abort();
            this.xmlhttprequest = $.ajax({
                type: "GET",
                url: url,
                data: params,
                dataType: (jsonp ? "jsonp" : "json"),/*
                jsonp: (jsonp ? "jsonp_callback" : false),
                jsonpCallback: 'callbackName',*/
                success: function(data, textStatus, jqXHR) {
                    _Cache.add(key, data);
                    fSuccess(data);
                    ajaxAnimation.removeClass(xhrName);
                    if (!ajaxAnimation.attr('class')) ajaxAnimation.fadeOut(400);
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    ajaxAnimation.removeClass(xhrName);
                    if (!ajaxAnimation.attr('class')) ajaxAnimation.fadeOut(400);
                    fFailure();
                    thisRef.requestErrorDialog(xhrName, textStatus, errorThrown);
                    //window.history.back();
                }
            });
            this.xmlhttprequest = null;
        }
    };
    
    // Get job subs from server
    this.ajax_getData_sync = function(xhrName, url, params, fSuccess, fFailure, obj) {
        if ( obj === undefined ) {
            obj = '';
        }
        var thisRef = this;
        
        currentUrl = window.location.toString()
        portIndex = currentUrl.indexOf('?port=');
        if (portIndex > -1) {
            
            port = ''           
            isNumber = true;    
            index = portIndex + 6       
        
            while(isNumber){
                char = currentUrl[index];
                if(char == '0' || char == '1' || char =='2' ||
                   char == '3' || char == '4' || char =='5' ||
                   char == '6' || char == '7' || char =='8' || char =='9'){
                    port = port + currentUrl[index];
                    index++;
                }
                else{
                    isNumber = false
                }
                
            }

            url = this.addPortNumber(url, port);            
        }
        
        var paramsString = '';
        for (x in params) { paramsString += x+'='+params[x]+'&' }
        var key = $.base64Encode(xhrName+'^'+url+'#'+paramsString);

        //ajaxAnimation.addClass(xhrName).show();
        var data = _Cache.get(key);
        if (data) {
            fSuccess(data, obj);
        } else if (url) {
            $.ajax({
                type: "GET",
                url: url,
                async: false,
                timeout: 15000,
                data: params,
                dataType: (jsonp ? "jsonp" : "json"),
                //jsonp: "jsonp_callback",
                success: function(data, textStatus, jqXHR) {
                    _Cache.add(key, data);
                    fSuccess(data, obj);
                    //ajaxAnimation.removeClass(xhrName);
                    //if (!ajaxAnimation.attr('class')) ajaxAnimation.hide();
                },
                error: function(jqXHR, textStatus, errorThrown) {
                    ajaxAnimation.removeClass(xhrName);
                    //if (!ajaxAnimation.attr('class')) ajaxAnimation.hide();
                    //fFailure(obj);
                }
            });
        }
    };
}

var simpleEncoding = 
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

// This function scales the submitted values so that
// maxVal becomes the highest value.
function simpleEncode(valueArray,maxValue) {
  var chartData = ['s:'];
  for (var i = 0; i < valueArray.length; i++) {
    var currentValue = valueArray[i];
    if (!isNaN(currentValue) && currentValue >= 0) {
    chartData.push(simpleEncoding.charAt(Math.round((simpleEncoding.length-1) * 
      currentValue / maxValue)));
    }
      else {
      chartData.push('_');
      }
  }
  return chartData.join('');
}

// Same as simple encoding, but for extended encoding.
var EXTENDED_MAP=
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.';
var EXTENDED_MAP_LENGTH = EXTENDED_MAP.length;
function extendedEncode(arrVals, maxVal) {
  var chartData = '';

  for(i = 0, len = arrVals.length; i < len; i++) {
    // In case the array vals were translated to strings.
    var numericVal = new Number(arrVals[i]);
    // Scale the value to maxVal.
    var scaledVal = Math.floor(EXTENDED_MAP_LENGTH * 
        EXTENDED_MAP_LENGTH * numericVal / maxVal);

    if(scaledVal > (EXTENDED_MAP_LENGTH * EXTENDED_MAP_LENGTH) - 1) {
      chartData += "..";
    } else if (scaledVal < 0) {
      chartData += '__';
    } else {
      // Calculate first and second digits and add them to the output.
      var quotient = Math.floor(scaledVal / EXTENDED_MAP_LENGTH);
      var remainder = scaledVal - EXTENDED_MAP_LENGTH * quotient;
      chartData += EXTENDED_MAP.charAt(quotient) + EXTENDED_MAP.charAt(remainder);
    }
  }

  return chartData;
}

function copyObj(o) {
    function F() {}
    F.prototype = o;
    return new F();
}

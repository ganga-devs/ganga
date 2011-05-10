// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 18.05.2010 Created
//

function Data(ajaxAnimation, settings, jsonp) {
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
        }
    };
   
    this.quickSetup = function(params, ts2iso) {
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

       
        var key = $.base64Encode($.param.querystring(url, params, 2));
       
        var data = _Cache.get(key);
        if (data) {
            fSuccess(data);
        } else if (url) {
            ajaxAnimation.addClass(xhrName).show();;
            $.ajax({
                type: "GET",
                url: url,
                data: params,
                dataType: (jsonp ? "jsonp" : "json"),
                jsonp: "jsonp_callback",
                success: function(data) {
                    _Cache.add(key, data);
                    fSuccess(data);
                    ajaxAnimation.removeClass(xhrName);
                    if (!ajaxAnimation.attr('class')) ajaxAnimation.hide();
                },
                error: function() {
                    ajaxAnimation.removeClass(xhrName);
                    if (!ajaxAnimation.attr('class')) ajaxAnimation.hide();
                    fFailure();
                }
            });
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
       
        var key = $.base64Encode($.param.querystring(url, params, 2));

        //ajaxAnimation.addClass(xhrName).show();
        var data = _Cache.get(key);
        if (data) {
            fSuccess(data);
        } else if (url) {
            $.ajax({
                type: "GET",
                url: url,
                async: false,
                timeout: 15000,
                data: params,
                dataType: (jsonp ? "jsonp" : "json"),
                jsonp: "jsonp_callback",
                success: function(data) {
                    fSuccess(data, obj);
                    //ajaxAnimation.removeClass(xhrName);
                    //if (!ajaxAnimation.attr('class')) ajaxAnimation.hide();
                },
                error: function() {
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

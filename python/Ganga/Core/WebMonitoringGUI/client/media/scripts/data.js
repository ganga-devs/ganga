// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// History:
// 18.05.2010 Created
//

function Data(ajaxAnimation) {
    // general values
    this.user = '';
    this.from = 0;
    this.till = 0;
    this.timeRange = 'lastDay';
    this.refresh = 0;
    this.tid = '';
    this.p = 1;
    this.sorting = [1,'desc'];
    this.or = []; // opened table rows
    this.uparam = []; // user defined params (for params that cannot be shared between use cases)
    
    this.noreload = false;
        
    // Data
    this.mem = {
        users: Array(),
        tasks: {
            user: '',
            timestamp: 0,
            data: Array()
        },
        jobs: {
            user: '',
            tid: '',
            timestamp: 0,
            data: Array()
        }
    };
    
    this.quickSetup = function(params, ts2iso) {
        this.user = (params['user'] || '');
        this.from = parseInt(this.iso2ts(params['from']) || 0);
        this.till = parseInt(this.iso2ts(params['till'],2) || 0);
        this.timeRange = ( (params['timeRange'] == '') ? params['timeRange'] : (params['timeRange'] || 'lastDay') );
        this.refresh = (params['refresh'] || 0);
        this.tid = (params['tid'] || '');
        this.p = (params['p'] || 1);
        this.or = (params['or'] || []);
        this.uparam = (params['uparam'] || []);
        
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
    
    // Get job subjobs from server
    this.ajax_getData = function(url, params, fSuccess, fFailure) {
        var thisRef = this;

        if (window.location.port) url = this.addPortNumber(url, window.location.port);
        
        var key = $.base64Encode($.param.querystring(url, params, 2));
        
        var data = _Cache.get(key);
        if (data) {
            fSuccess(data);
        } else {
            ajaxAnimation.show();
            $.ajax({
                type: "GET",
                url: url,
                data: params,
                dataType: "jsonp",
		jsonp: 'jsonp_callback',
                success: function(data) {
                    _Cache.add(key, data);
                    fSuccess(data);
                    ajaxAnimation.hide();
                },
                error: function() {
                    ajaxAnimation.hide();
                    fFailure();
                }
            });
        }
    };
}

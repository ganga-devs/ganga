function Model() {
    this.tableData;  // Data that is used to build table structure
    
    // Save tableData value
    this.setTableData = function(data) {
        this.tableData = data;
    };
    
    // Handle ajax error
    this.ajax_error = function() {
        $('#ajaxLoading').hide();
        alert('Error during Ajax request...');
    }
    
    // Get table data from server
    this.ajax_getTableData = function(f, user, query, from, till) {
        var thisRef = this; // Creating object reference to use if inside a function
        var base64query = '';
        if (query != '') base64query = '&query='+$.base64Encode(query);
        else base64query = '';
        
        // Formatting date query
        var from_tmstmp = $.datepicker.formatDate('@', new Date(from))/1000;
        var till_tmstmp = ($.datepicker.formatDate('@', new Date(till))/1000)+86399;
        
        var from_url = (from != '' ? '&from='+from_tmstmp : '');
        var till_url = (till != '' ? '&till='+till_tmstmp : '');
        
        if ((from != '' && till != '') && till_tmstmp < from_tmstmp) {
            alert('Date input error!');
            return false;
        }
        
        /*if (user == '') {
            do {
                user = prompt("Please enter your username");
            }
            while (user == '');
            $('#input-user').attr('value', user);
        }*/
        
        _sub_cache.clear(); // clear cache
        $('#ajaxLoading').show();
        $.ajax({
            type: "GET",
            url: settings_HOST+settings_PORT+"/gangajobs?user="+user+from_url+till_url+base64query,
            dataType: "json",
            success: function(data) {
                $('#ajaxLoading').hide();
                f(data);
            },
            error: thisRef.ajax_error
        });
    };
    
    // Get job subjobs from server
    this.ajax_getSubjobs = function(f, job_uuid) {
        var thisRef = this; // Creating object reference to use if inside a function
        var key = "subjobs_"+job_uuid;   // unique key for the cache
        
        // load data form cache
        var data = _sub_cache.get(key);
        if (data) { f(data); return; }
        
        $('#ajaxLoading').show();
        $.ajax({
            type: "GET",
            url: settings_HOST+settings_PORT+"/gangadetails?job_uuid="+job_uuid,
            dataType: "json",
            success: function(data) {
                _sub_cache.add(key, data, 5);
                $('#ajaxLoading').hide();
                f(data); 
            },
            error: thisRef.ajax_error
        });
    };
}

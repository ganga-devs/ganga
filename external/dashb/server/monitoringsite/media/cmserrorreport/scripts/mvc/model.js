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
	
	var querystr = '&query='+query;
       
        // Formatting date query
        var from_tmstmp = $.datepicker.formatDate('@', new Date(from))/1000;
        var till_tmstmp = ($.datepicker.formatDate('@', new Date(till))/1000)+86399;
        
        var from_url = (from != '' ? '&from='+from_tmstmp : '');
        var till_url = (till != '' ? '&till='+till_tmstmp : '');
        
        if ((from != '' && till != '') && till_tmstmp < from_tmstmp) {
	    $('#ajaxLoading').hide();	
            alert('Date input error!');
            return false;
        }

	var appFolder = "errorreports"

	if (settings_COMMUNITY == 'CMS')
	{
		appFolder = "cmserrorreports"
	}

        $('#ajaxLoading').show();
        $.ajax({
            type: "GET",
            url: settings_HOST+settings_PORT+ "/" + appFolder + "/get_reports_JSON?user="+user+from_url+till_url+querystr,
            dataType: "json",
            success: function(data) {
                $('#ajaxLoading').hide();
                f(data);
            },
            error: this.ajax_error
        });

    };
    }

function Controller(model, view) {
    this.dTable; // Data Tables object
    
    // Table redraw function
    this.getTableData = function(data) {
        // Save data in model
        model.setTableData(data);
        this.drawTable();
    }
    
    // prevents object reference, maybe not the best way to do it (think about it later)
    this.copy = function(obj) {
        return $.evalJSON($.toJSON(obj));
    };
    
    this.showLoadingTableDialog = function() {
        var tableWidth = 0, dialogLeft = 0;
        // info dialog open
        tableWidth = $('#tableDiv').width();
		dialogLeft = (tableWidth/2 - 150)+'px';
		$('#dialog').css('left', dialogLeft);
		$('#loadingTable').show();
    };
    
    // Draw data table
    this.drawTable = function() {
        // Copying object becouse we don't want to change orginal
        var tableData = this.copy(model.tableData);
        var tableDataLength = tableData.length;
        this.dTable.fnClearTable( 1 ); // Clearing Table

        // Redraw only if new data exists
        if (tableDataLength > 0) {
            
            // Draw table
            this.dTable.fnAddData(tableData, false);
            this.dTable.fnDraw();
            if (param('p')) {
                $('#url-page').trigger('click');
            }
        }
        // info dialog close
        $('#loadingTable').hide();
    };
    
    // Initialize function
    // Setting up model and events
    this.initialize = function() {
        var thisRef = this; // Creating object reference to use if inside a function
	var queryDecoded = '';

        //$.fragmentChange( true );

	// Activate datepicker
        $('#from, #till').datepicker({
                        changeMonth: true,
                        changeYear: true
                });

	$('#loadingTable').hide();

        // dataTable script initialization using jQuery selectors
        this.dTable = $('#dataTable').dataTable( { 
            "iDisplayLength":25,
            "sPaginationType": "input",
            "bAutoWidth": false,
            "aaSorting": [[0,'desc']],
            "aoColumns" : [
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' }, 
		{ sWidth: 'auto',"bSortable": false },
		{ sWidth: 'auto',"bSortable": false },
            ]  
        } );
  
        this.dTable.fnClearTable( 0 ); // Clearing Table
	
	// Decoding query for URL
        if (param('query')) {
            queryDecoded = $.base64Decode(param('query'));
        }
        
        // Get data
        this.showLoadingTableDialog();
        model.ajax_getTableData( function(data) { thisRef.getTableData(data) }, (param('user') || ''), queryDecoded,  (param('from') || ''), (param('till') || '') );
	
	// Filling up form
        $('#input-user').attr('value', (param('user') || ''));
        $('#input-query').attr('value', queryDecoded);
        $('#from').attr('value', (param('from') || ''));
        $('#till').attr('value', (param('till') || ''));
	
	$('#loadingTable').hide();
                
        // Setting up events
        $('#dataTable thead tr,#dataTable_next,#dataTable_previous,#dataTable_first,#dataTable_last').click( function(){ setFragment(1);});
        $('#dataTable_paginate input').keyup( function() { setFragment(1);});
	
	$('#button-query').click( function() {
            setFragment(0);
            thisRef.showLoadingTableDialog();
            model.ajax_getTableData( function(data) { thisRef.getTableData(data) }, $('#input-user').attr('value'), $('#input-query').attr('value'), $('#from').attr('value'), $('#till').attr('value') );
        });
        $('#input-user, #input-query, #from, #till').keypress( function(event){
            if (event.keyCode == '13') {
                setFragment(0);
                thisRef.showLoadingTableDialog();
                model.ajax_getTableData( function(data) { thisRef.getTableData(data) }, $('#input-user').attr('value'), $('#input-query').attr('value'), $('#from').attr('value'), $('#till').attr('value') );
            }
        });

        // Set up URL
	var  setFragment = function(page) {
            $.setFragment('user='+$('#input-user').attr('value')+
                ($('#from').attr('value') != '' ? '&from='+$('#from').attr('value') : '')+
                ($('#till').attr('value') != '' ? '&till='+$('#till').attr('value') : '')+
                //($('#input-query').attr('value') != '' ? '&query='+escape($.base64Encode($('#input-query').attr('value'))) : '')+
		($('#input-query').attr('value') != '' ? '&query='+$('#input-query').attr('value') : '')+
                (page ? '&p='+$('#dataTable_paginate input').attr('value') : '')
            , 2);
        }
        //var setFragment = function(page) {
        //    $.setFragment((page ? '&p='+$('#dataTable_paginate input').attr('value') : ''), 2);
        //}
    };
}


// parse parameters from URL and make them available as globals param("name")
(function()
{
    var s = window.location.hash.substring(1).split('&');
    var c = {};
    for (var i  = 0; i < s.length; i++) {
        var parts = new Array();
        parts[0] = s[i].split('=', 1);
        parts[1] = s[i].replace(parts[0]+'=', '');
        c[unescape(parts[0])] = unescape(parts[1]);
    }
    window.param = function(name) { return name ? c[name] : false; };
})();


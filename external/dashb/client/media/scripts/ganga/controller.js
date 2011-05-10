function Controller(model, view) {
    this.dTable; // Data Tables object
    
    // Table tr click handler
    this.mainTableTr_click = function(el, data, trID) {
        var thisRef = this;
        // Update status on a master job
        this.dTable.fnUpdate( data.job_details.status, trID, 5, false );
    
        // put another row after clicked row
        $(el).after(view.createSubJobRow(trID, data, model.tableData));
        view.drawSubPieStatus(data, trID);
        view.drawSubPieHosts(data, trID);
        
        // show new row, animate
        $('#subjobs_'+trID).hide().show('blind');
        
        // add refresh event
        $('#refresh_'+trID).click( function() {
            //alert(';)');
            model.ajax_getSubjobs(function(data) {
                $('#reload_'+trID).html(view.createSubJobData(trID, data, model.tableData));
                view.drawSubPieStatus(data, trID);
                view.drawSubPieHosts(data, trID);
                // Update status on a master job
                thisRef.dTable.fnUpdate( data.job_details.status, trID, 5, false );
            }, model.tableData[trID][8]);
        });
    }
    
    // Table redraw function
    this.getTableData = function(data, updatePie) {
        // Save data in model
        model.setTableData(data);
        this.drawTable();
        view.drawPieStatus(updatePie);
        view.drawPieHosts(updatePie);
    }
    
    // Reset table rows events
    this.resetTableEvents = function() {
        var thisRef = this; // Creating object reference to use if inside a function
        var dataTableRows = $('#dataTable tbody tr');
        dataTableRows.unbind();
        dataTableRows.click( function() {
            var clickRef = this;
            var trID = thisRef.dTable.fnGetPosition( this );
            if ($('#subjobs_'+trID).length == 0) {
                $('#tablePlus_'+trID).attr('src', view.tableMinus);
                model.ajax_getSubjobs(function(data) {
                    thisRef.mainTableTr_click(clickRef, data, trID);
                }, model.tableData[trID][8]);
            }
            else {
                $('#tablePlus_'+trID).attr('src', view.tablePlus);
                $('#subjobs_'+trID).hide('blind', function() {$('#additional_'+trID).remove()});
            }
        });
    };
    
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
            // Adding first column with + sign
            for (var i=0; i<tableDataLength; i++) {
                tableData[i].unshift(view.addPlusColumn(i));
            }
            // Draw table
            this.dTable.fnAddData(tableData, false);
            this.dTable.fnDraw();
            if (param('p')) {
                $('#url-page').trigger('click');
            }
            this.resetTableEvents(); // Reset event for table rows
        }
        // info dialog close
        $('#loadingTable').hide();
    };
    
    // Initialize function
    // Setting up model and events
    this.initialize = function() {
        var thisRef = this; // Creating object reference to use if inside a function
        var queryDecoded = '';
        
        $.fragmentChange( true );
        
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
            "aaSorting": [[1,'desc']],
            "aoColumns" : [
                { sWidth: '10px', "bSortable": false },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto' },
                { sWidth: 'auto', bVisible: false }
            ]  
        } );  
        this.dTable.fnClearTable( 0 ); // Clearing Table
        
        // Decoding query for URL
        if (param('query')) {
            queryDecoded = $.base64Decode(param('query'));
        }
        
        // Get data
        this.showLoadingTableDialog();
        model.ajax_getTableData( function(data) { thisRef.getTableData(data, false) }, (param('user') || ''), queryDecoded, (param('from') || ''), (param('till') || '') );
        
        // Filling up form
        $('#input-user').attr('value', (param('user') || ''));
        $('#input-query').attr('value', queryDecoded);
        $('#from').attr('value', (param('from') || ''));
        $('#till').attr('value', (param('till') || ''));
        
        // Setting up events
        $('#dataTable thead tr,#dataTable_next,#dataTable_previous,#dataTable_first,#dataTable_last').click( function() {thisRef.resetTableEvents(); setFragment(1);});
        $('#dataTable_paginate input').keyup( function() {thisRef.resetTableEvents(); setFragment(1);});
        $('#dataTable_length select,#dataTable_filter input').change( function() {thisRef.resetTableEvents()});
        $('#button-query').click( function() {
            setFragment(0);
            thisRef.showLoadingTableDialog();
            model.ajax_getTableData( function(data) { thisRef.getTableData(data, true) }, $('#input-user').attr('value'), $('#input-query').attr('value'), $('#from').attr('value'), $('#till').attr('value') );
        });
        $('#input-user, #input-query, #from, #till').keypress( function(event){
            if (event.keyCode == '13') {
                setFragment(0);
                thisRef.showLoadingTableDialog();
                model.ajax_getTableData( function(data) { thisRef.getTableData(data, true) }, $('#input-user').attr('value'), $('#input-query').attr('value'), $('#from').attr('value'), $('#till').attr('value') );
            }
        });
        $('#switchAppLink').click( function() { 
            var url = 'diane.html';
            if ($('#input-user').attr('value') != '')
                url += '#user='+$('#input-user').attr('value'); 
            location.href = url;
        });
        
        // Set up URL
        var setFragment = function(page) {
            $.setFragment('user='+$('#input-user').attr('value')+
                ($('#from').attr('value') != '' ? '&from='+$('#from').attr('value') : '')+
                ($('#till').attr('value') != '' ? '&till='+$('#till').attr('value') : '')+
                ($('#input-query').attr('value') != '' ? '&query='+escape($.base64Encode($('#input-query').attr('value'))) : '')+
                (page ? '&p='+$('#dataTable_paginate input').attr('value') : '')
            , 2);
        }
    };
}

function distArray(item, arr) {
    var arrLength = arr.length;
    var itemsEx = 'true';
    
    for (var i=0; i<arrLength; i++) {
        //alert(i+':'+(arr[i] == item)+'#'+arr[i]+';'+item);
        if (arr[i] == item) {
            itemsEx = i;
            break;
        }
    }
    
    return itemsEx;
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

// This file is part of the jTaskMonitoring software
// Copyright (c) CERN 2010
//
// Author: Lukasz Kokoszkiewicz [lukasz@kokoszkiewicz.com , lukasz.kokoszkiewicz@cern.ch]
//
// Requirements:
// - jquery.dataTables.min.js
// - jquery.ba-bbq.min.js
//
// History:
// 18.05.2010 Created
// 17.01.2011 First production release (v1.0.0)
//

(function($) {
    $.fn.lkfw_dataTable = function(settings) {
        var _config = {
            'dTable': [],
            'tableId': 'srchList',
            'items': [],
            'tblLabels': [],
            'dataTable':{},
            'expandableRows':false,
            'multipleER':false,   // Multiple expandable rows
            'rowsToExpand':[],
            'sorting':[0,'desc'],
            'fnERContent':function(dataID, onlyData){ return {'properties':[['error','Data provider function not set up!']],'table':false,'html':false} },
            'fnERContentPostProcess':function(expandedID){},
            'fnContentChange':function(el){ alert('Please define a proper function to handle "fnContentChange"!'); },
            'fnERClose':function(dataID){ alert('Please define a proper function to handle "fnERClose"!'); },
            'fnTableSorting':function(el){  }
        };
        
        var _tablePlus = 'media/images/table_plus.png';
        var _tableMinus = 'media/images/table_minus.png';
        
        var _buildTable = function(elCnt) {
            var table = $('<table></table>').attr({
                id: 'dataTable_'+elCnt,
                cellpadding: '0',
                cellspacing: '1'
            }).addClass('display');
            var colHeaders = $('<tr></tr>');
            for (i in _config.tblLabels) {
                var colHeader = $('<th></th>').text(_config.tblLabels[i]);
                if (!_config.expandableRows || i!=0) colHeader.addClass('tblSort');
                colHeaders.append(colHeader);
            }
            var colFooters = colHeaders.clone();
            var tblHead = $('<thead></thead>').append(colHeaders);
            var tblFoot = $('<tfoot></tfoot>').append(colFooters);
            
            table.append(tblHead);
            table.append(tblFoot);
            
            return table;
        };
        
        var _buildExpandedRow = function(trID, trClass, inputObj) {
            //var inputObj = _config.fnERContent(trID[0]);
            var tr, td, tdKEY, tdVAL;
            
            var mainTR = $('<tr></tr>').attr({
                'id': 'expand_'+trID[0]
            }).addClass('expand').addClass(trClass);
            var mainTD = $('<td></td>').attr({
                'colspan': _config.tblLabels.length
            }).addClass('sorting_1');
            
            for (var j=0;j<inputObj.length;j++) {
                // Building properties table - start
                if (inputObj[j][0] == 'properties' && inputObj[j][1]) {
                    var properties = inputObj[j][1];
                    var propertiesTable = $('<table></table>').attr({
                        cellpadding: '0',
                        cellspacing: '1'
                    }).addClass('expTable');
                    
                    for (i in properties) {
                        tr = $('<tr></tr>');
                        tdKEY = $('<td></td>').addClass('orKEYS').text(properties[i][0]);
                        tdVAL = $('<td></td>').addClass('orVALS').text(properties[i][1]);
                        tr.append(tdKEY).append(tdVAL);
                        propertiesTable.append(tr);
                    }
                    mainTD.append(propertiesTable);
                }
                // Building properties table - finish
                
                // Adding custom html
                else if (inputObj[j][0] == 'html' && inputObj[j][1]) mainTD.append(inputObj[j][1]);
                
                // Building data table - start
                else if (inputObj[j][0] == 'table' && inputObj[j][1]) {
                    var table = inputObj[j][1];
                    var dataTable = $('<table></table>').attr({
                        id: 'expandDataTable_'+trID[0],
                        cellpadding: '0',
                        cellspacing: '1'
                    }).addClass('display').addClass('expandDataTable').css('margin-bottom','10px');
                    var colHeaders = $('<tr></tr>');
                    for (i in table.tblLabels) {
                        var colHeader = $('<th></th>').html(table.tblLabels[i]);
                        colHeaders.append(colHeader);
                    }
                    //var colFooters = colHeaders.clone();
                    var tblHead = $('<thead></thead>').append(colHeaders);
                    var tblBody = $('<tbody></tbody>');
                    
                    var evenOdd = 3;
                    for (var i=0;i<table.tblData.length;i++) {
                        if ((evenOdd % 2) == 1) var evenOddClass = 'odd';
                        else var evenOddClass = 'even';
                        tr = $('<tr></tr>').addClass(evenOddClass).addClass('gradeU');
                        for (var j=0;j<table.tblData[i].length;j++) {
                            tr.append($('<td></td>').addClass('expDataTableTd').html(table.tblData[i][j]));
                        }
                        tblBody.append(tr);
                        evenOdd++;
                    }
                    
                    //var tblFoot = $('<tfoot></tfoot>').append(colFooters);
                    
                    dataTable.append(tblHead);
                    dataTable.append(tblBody);
                    //dataTable.append(tblFoot);
                    
                    mainTD.append(dataTable);
                }
                // Building data table - finish
                
                // Setting up charts div - start
                else if (inputObj[j][0] == 'charts' && inputObj[j][1]) {
                    mainTD.append($('<div></div>').css('width','100%').attr('id','chartExpandSlot_'+trID[0]));
                }
                // Setting up charts div - finish
            }
            
            mainTR.append(mainTD);
            
            return mainTR;
        };
        
        var _givPlus = function(iteration) {
            return '<img id="tablePlus_'+iteration+'" class="tablePlus" src="'+_tablePlus+'" />';
        };
        
        var _expandClick = function(dTable) {
            $('.rExpand').unbind();
            $('.rExpand').click(function(){
                var trID = dTable.fnGetPosition( this );
                if (_config.multipleER) {
                    if ($('#expand_'+trID[0]).length == 0) {
                        $('#tablePlus_'+trID[0]).attr('src', _tableMinus);
                        // Create row
                        var inputObj = _config.fnERContent(trID[0]);
                        $(this).parent().after(_buildExpandedRow(trID, $(this).parent().attr('class'), inputObj));
                        _config.fnERContentPostProcess(trID[0], inputObj);
                    }
                    else {
                        _config.fnERClose(trID[0]);
                        $('#tablePlus_'+trID[0]).attr('src', _tablePlus);
                        $('#expand_'+trID[0]).remove();
                    }
                }
                else {
                    // Close other
                    var isNotCurrent = ($('#expand_'+trID[0]).length == 0);
                    $('.expand').remove();
                    $('.tablePlus').attr('src', _tablePlus);
                    
                    // Open current
                    if (isNotCurrent) {
                        $(this).children('.tablePlus').attr('src', _tableMinus);
                        var inputObj = _config.fnERContent(trID[0]);
                        $(this).parent().after(_buildExpandedRow(trID, $(this).parent().attr('class'), inputObj));
                        _config.fnERContentPostProcess(trID[0], inputObj);
                    }
                    else {
                        _config.fnERClose(trID[0]);
                    }
                }
            });
        };
 
        if (settings) $.extend(_config, settings);
        
        if (_config.expandableRows) {
            // Adding first column with + sign
            // Setting up column settings if they are not exists
            if (!_config.dataTable.aoColumns) {
                _config.dataTable.aoColumns = Array();
                for (i in _config.tblLabels) {
                    _config.dataTable.aoColumns.push(null);
                }
            }
            // Adding empty column label
            _config.tblLabels = $.merge([''], _config.tblLabels);
            
            // Adding PLUS image to every row
            for (var i=0; i<_config.items.length; i++) {
                _config.items[i] = $.merge([_givPlus(i)], _config.items[i]);
            }
            
            // Setting up PLUS column
            _config.dataTable.aoColumns = $.merge([{ 
                sWidth:'10px',
                bSortable:false, 
                sClass:'rExpand'
            }],_config.dataTable.aoColumns);
        }
        
        var elCnt = 0;
        var dTablesArr = Array();
        this.each(function() {
		    dTable = _config.dTable[elCnt];
            if (!dTable) {
                $(this).empty().append(_buildTable(elCnt));
                dTable = $('#dataTable_'+elCnt).dataTable( $.extend({
					    "bJQueryUI": false,
					    "sPaginationType": "full_numbers",
					    "bAutoWidth":false,
					    "bSortClasses": true,
					    "aaSorting": [[_config.sorting[0],_config.sorting[1]]]
		        },_config.dataTable));
		    }
		    else {
		        dTable.fnClearTable();
		    }
		    dTable.fnAddData(_config.items);
		    dTablesArr.push(dTable);
		    
		    // Setting up table events
		    if (_config.dataTable.sPaginationType) {
                $('#dataTable_'+elCnt+' thead tr,#dataTable_'+elCnt+'_next,#dataTable_'+elCnt+'_previous,#dataTable_'+elCnt+'_first,#dataTable_'+elCnt+'_last').click( function() { _config.fnContentChange(this); if (_config.expandableRows) _expandClick(dTable); } );
                $('#dataTable_'+elCnt+'_paginate input,#dataTable_'+elCnt+'_filter input').keyup( function() { _config.fnContentChange(this); if (_config.expandableRows) _expandClick(dTable); } );
            }
            if (_config.expandableRows) _expandClick(dTable);
            $('.tblSort').click( function() { _config.fnTableSorting(this); } );
            //_expandInit(dTable);
            elCnt++;
        });
        
        return dTablesArr;
    };
})(jQuery);

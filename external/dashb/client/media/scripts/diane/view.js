function View() {
    this.tablePlus = 'media/images/table_plus.png';
    this.tableMinus = 'media/images/table_minus.png';
    
    // Add plus column
    this.addPlusColumn = function(iteration) {
        return '<img id="tablePlus_'+iteration+'" src="'+this.tablePlus+'" />';
    }
    
    // Create Details row
    this.createSubJobRow = function(trID, data, tableData) {
        var rows = '';
        
        rows += '<tr id="additional_'+trID+'"><td colspan="9" style="border-top:4px #dddddd solid;">';
        rows += '<div id="subjobs_'+trID+'" class="tableAdditionalInfo">';
        rows += '<div style="position:relative"><div style="position:absolute; top:0px; right:0px;">';
        rows += '<a style="color:#0C6DFF" href="ganga.html#user='+$('#input-user').attr('value')+'&query='+escape($.base64Encode('diane_master_uuid:'+tableData[trID][8]))+'">show ganga jobs</a>';
        rows += '</div></div>';
        rows += '<b>run_id:</b> '+data.run_id+'<br />';
        rows += '<b>master_uuid:</b> '+tableData[trID][8]+'<br />';
        rows += '</div>';
        rows += '</td></tr>';
        return rows;
    }
}

// Inherit from Charts (charts.js must be included in HTML file)
View.prototype = new Charts();

function View() {
    this.tablePlus = 'media/images/table_plus.png';
    this.tableMinus = 'media/images/table_minus.png';
    
    // Add plus column
    this.addPlusColumn = function(iteration) {
        return '<img id="tablePlus_'+iteration+'" src="'+this.tablePlus+'" />';
    };
    
    // Create Details row
    this.createSubJobRow = function(trID, data, tableData) {
        var rowsArrLength = data.subjobs.length;
        var rows = '';
        
        rows += '<tr id="additional_'+trID+'"><td colspan="9" style="border-top:4px #dddddd solid">';
        rows += '<div id="subjobs_'+trID+'" class="tableAdditionalInfo">';
        rows += '<div style="position:relative"><div style="position:absolute; top:0px; right:0px;">';
        rows += '<input id="refresh_'+trID+'" type="button" value="Refresh" />';
        rows += '</div></div>';
        rows += this.createSubJobData(trID, data, tableData);
        rows += '</div>';
        rows += '</td></tr>';
        return rows;
    };
    
    this.createSubJobData = function(trID, data, tableData) {
        var rowsArrLength = data.subjobs.length;
        var rows = '';
        
        rows += '<div id="reload_'+trID+'">';
        rows += '<b>job_uuid:</b> '+tableData[trID][8]+'<br />';
        rows += '<b>user:</b> '+data.job_details.user+'<br />';
        rows += '<b>repository:</b> '+data.job_details.repository+'<br />';
        if (rowsArrLength > 0) {
            rows += '<div id="pieChart_'+trID+'" style="display:inline; width: 500px; height: 200px;"></div>';
            rows += '<div id="pieChart2_'+trID+'" style="display:inline; width: 500px; height: 200px;"></div><br />';
            rows += '<b>Subjobs:</b>';
            rows += '<table cellpadding="0" cellspacing="1" border="0" class="subTable">';
            rows += '<tr><thead>';
            rows += '<th>Time</th>';
            rows += '<th>Id</th>';
            rows += '<th>Name</th>';
            rows += '<th>Status</th>';
            rows += '<th>Application</th>';
            rows += '<th>Backend</th>';
            rows += '<th>Workernode</th>';
            rows += '</tr></thead>';
            rows += '<tbody>';
            for (var i=1; i<=rowsArrLength; i++) {
                rows += '<tr class="gradeA '+((i%2)==0?"odd":"even")+'">';
                rows += '<td>'+data.subjobs[(i-1)][0]+'</td>';
                rows += '<td>'+data.subjobs[(i-1)][1]+'</td>';
                rows += '<td>'+data.subjobs[(i-1)][2]+'</td>';
                rows += '<td>'+data.subjobs[(i-1)][3]+'</td>';
                rows += '<td>'+data.subjobs[(i-1)][4]+'</td>';
                rows += '<td>'+data.subjobs[(i-1)][5]+'</td>';
                rows += '<td>'+data.subjobs[(i-1)][6]+'</td>';
                rows += '</tr>';
            }
            rows += '</tbody></table><br />';
        }
        rows += '</div>';
        
        return rows;
    };
}

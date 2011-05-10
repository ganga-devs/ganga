function Charts() {
    this.drawCharts = new drawCharts();

    // Draw pie chart
    this.drawPieStatus = function(update) {
        var tableData = model.tableData;
        var tableDataLength = tableData.length;
        var running = 0, finished = 0, failed = 0, submitted = 0, incomplete = 0, master_job = 0;
        var labels, title;
        
        // Counting parameters
        for (var i=0; i<tableDataLength; i++) {
            switch (tableData[i][4]) {
                case 'submitted':
                    submitted++;
                    break;
                case 'running':
                    running++;
                    break;
                case 'finished':
                    finished++;
                    break;
                case 'failed':
                    failed++;
                    break;
                case 'incomplete':
                    incomplete++;
                    break;
                case 'master job':
                    master_job++;
                    break;
            }
        }
        
        // Setting up data
        var data = [$.gchart.series('query', [submitted, running, finished, failed, incomplete, master_job])]; 
        data[0].color = ['87A5FF', 'lime', 'blue', 'red', 'orange', 'FF6100'];
        
        title = 'Status Overview ('+tableDataLength+' '+((tableDataLength>1) ? 'jobs' : 'job')+')';
        labels = [(submitted ? 'Submitted ('+submitted+')' : ''), (running ? 'Running ('+running+')' : ''), (finished ? 'Finished ('+finished+')' : ''), (failed ? 'Failed ('+failed+')' : ''), (master_job ? 'Master job ('+master_job+')' : '')];
        
        this.drawCharts.drawPie(update, '#pieChart', data, labels, title);
    };
    
    // Draw pie chart for subjobs
    this.drawSubPieStatus = function(data, trID) {
        var tableData = data.subjobs;
        var tableDataLength = tableData.length;
        var running = 0, finished = 0, failed = 0, submitted = 0, incomplete = 0, master_job = 0;
        var labels, title;
        
        // Counting parameters
        for (var i=0; i<tableDataLength; i++) {
            switch (tableData[i][3]) {
                case 'submitted':
                    submitted++;
                    break;
                case 'running':
                    running++;
                    break;
                case 'finished':
                    finished++;
                    break;
                case 'failed':
                    failed++;
                    break;
                case 'incomplete':
                    incomplete++;
                    break;
                case 'master job':
                    master_job++;
                    break;
            }
        }
        // Setting up data
        var data = [$.gchart.series('query', [submitted, running, finished, failed, incomplete, master_job])]; 
        data[0].color = ['87A5FF', 'lime', 'blue', 'red', 'orange', 'FF6100'];
        
        title = 'Status Overview ('+tableDataLength+' '+((tableDataLength>1) ? 'subjobs' : 'subjob')+')';
        labels = [(submitted ? 'Submitted ('+submitted+')' : ''), (running ? 'Running ('+running+')' : ''), (finished ? 'Finished ('+finished+')' : ''), (failed ? 'Failed ('+failed+')' : ''), (master_job ? 'Master job ('+master_job+')' : '')];
        
        this.drawCharts.drawPie(false, '#pieChart_'+trID, data, labels, title, 'EEF0F1');
    };
    
    // Draw pie chart
    this.drawPieHosts = function(update) {
        var tableData = model.tableData;
        var tableDataLength = tableData.length;
        var ce = Array();
        var ceVal = Array();
        var totalCEs = 0;
        var title;
        
        // Counting parameters
        for (var i=0; i<tableDataLength; i++) {
            var distArr = distArray(tableData[i][7], ce);
            if(distArr == 'true') {
                ce.push(tableData[i][7]);
                ceVal.push(1);
                totalCEs++;
            }
            else {
                ceVal[distArr]++;
            }
        }
        
        if (ce.length > 10) {
            var go = true;
            ce.push('other CEs');
            ceVal.push(0);
            for (var i=1; (go && i<1000); i++) {
                var ids = Array();
                for (var j in ce) {
                    if (ceVal[j] == i) {
                        ids.push(j);
                    }
                }
                ids.sort( function(a,b) { return a-b; });
                ceVal[(ce.length-1)] += ids.length*i;
                for (x in ids) {
                    ce.splice(ids[x]-x, 1);
                    ceVal.splice(ids[x]-x, 1);
                }
                if (ce.length <= 10) {
                    go = false;
                }
            }
        }
        
        for (var i=0; i<ce.length; i++) {
            ce[i] = ce[i]+' ('+ceVal[i]+')';
        }
        
        // Setting up data
        var data = [$.gchart.series('query', ceVal)];
        
        title = "Hosts Overview ("+totalCEs+" "+((totalCEs>1) ? 'CEs' : 'CE')+")";
        
        this.drawCharts.drawPie(update, '#pieChart2', data, ce, title);
    };
    
    // Draw pie chart for subjobs
    this.drawSubPieHosts = function(data, trID) {
        var tableData = data.subjobs;
        var tableDataLength = tableData.length;
        var ce = Array();
        var ceVal = Array();
        var totalCEs = 0;
        var title;
        
        // Counting parameters
        for (var i=0; i<tableDataLength; i++) {
            var distArr = distArray(tableData[i][7], ce);
            if(distArr == 'true' && tableData[i][7] != '') {
                ce.push(tableData[i][7]);
                ceVal.push(1);
                totalCEs++;
            }
            else {
                ceVal[distArr]++;
            }
        }
        
        if (ce.length > 0) {
            if (ce.length > 10) {
                var go = true;
                ce.push('other CEs');
                ceVal.push(0);
                for (var i=1; (go && i<1000); i++) {
                    var ids = Array();
                    for (var j in ce) {
                        if (ceVal[j] == i) {
                            ids.push(j);
                        }
                    }
                    ids.sort( function(a,b) { return a-b; });
                    ceVal[(ce.length-1)] += ids.length*i;
                    for (x in ids) {
                        ce.splice(ids[x]-x, 1);
                        ceVal.splice(ids[x]-x, 1);
                    }
                    if (ce.length <= 10) {
                        go = false;
                    }
                }
            }
            
            for (var i=0; i<ce.length; i++) {
                ce[i] = ce[i]+' ('+ceVal[i]+')';
            }
            
            //alert(ce+'###'+ceVal);
            // Setting up data
            var data = [$.gchart.series('query', ceVal)];
            
            title = "Hosts Overview ("+totalCEs+" "+((totalCEs>1) ? 'Workernodes' : 'Workernode')+")";
            
            this.drawCharts.drawPie(false, '#pieChart2_'+trID, data, ce, title, 'EEF0F1');
        }
        
    };
}

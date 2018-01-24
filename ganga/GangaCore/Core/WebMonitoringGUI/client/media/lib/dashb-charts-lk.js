function drawCharts() {
    // Pie Chart
    this.drawPie = function(update, id, data, labels, title, bgcolor) {
        if (!bgcolor) bgcolor = '#ffffff'
    
        if (update) {
            // Update chart
            $(id).gchart('change',{series: data, title: title, dataLabels: labels});
        }
        else {
            // Draw chart
            $(id).gchart({type: 'pie3D', series: data, title: title, backgroundColor: bgcolor, dataLabels: labels});
        }
    }
}

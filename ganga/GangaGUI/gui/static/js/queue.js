var chart;

function requestData()
{
            
    var requests = $.get('/api/queue/chart');


    var tm = requests.done(function (result)
    {
        var series = chart.series[0],
            shift = series.data.length > 20;
        chart.series[0].addPoint(result, true, shift);
        setTimeout(requestData, 2000);
    });
}

$(document).ready(function() {
    chart = new Highcharts.Chart({
        chart: {
            renderTo: 'data-container',
            defaultSeriesType: 'spline',
            events: {
                load: requestData
            }
        },
        title: {
            text: 'Queue'
        },
        xAxis: {
            type: 'datetime',
            tickPixelInterval: 150,
            maxZoom: 20 * 1000
        },
        yAxis: {
            minPadding: 0.2,
            maxPadding: 0.2,
            title: {
                text: 'Worker_Threads',
                margin: 80
            }
        },
        series: [{
            name: 'Time',
            data: []
        }]
    });

});
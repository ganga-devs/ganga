document.addEventListener('DOMContentLoaded', () => {

    let ids = ["#new", "#running", "#completed", "#failed", "#killed"];

    let chartData = ids.map(id => parseInt(document.querySelector(id).innerHTML))

    let data = {
        datasets: [{
            data: chartData,
            backgroundColor: [
                'rgba(24, 162, 184, 1)',
                'rgba(0, 123, 255, 1)',
                'rgba(40, 167, 69, 1)',
                'rgba(220, 53, 69, 1)',
                'rgba(153, 102, 255, 1)',
            ],
        }],

        // These labels appear in the legend and in the tooltips when hovering different arcs
        labels: [
            'New',
            'Running',
            'Completed',
            'Failed',
            'Killed',
        ],


    };

    let ctx = document.getElementById('myChart').getContext('2d');
    let myPieChart = new Chart(ctx, {
        type: 'pie',
        data: data,

    });
});
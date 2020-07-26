document.addEventListener('DOMContentLoaded', () => {
    setInterval(refreshStatistics, 2000)
    setInterval(refreshRecentJobs, 2000)
});

// Function to plot chart
function plotStatChart() {
    // Get data from stat cards
    const statIds = ["#stat-new", "#stat-running", "#stat-completed", "#stat-failed", "#stat-killed"];
    const chartData = statIds.map(id => parseInt(document.querySelector(id).innerHTML));

    // Chart data
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

    // Plot chart
    let ctx = document.getElementById('statChart').getContext('2d');
    let myPieChart = new Chart(ctx, {
        type: 'pie',
        data: data,
    });
}

// Refresh stat cards of dashboard
function refreshStatistics() {

    let statuses = ["new", "running", "completed", "failed", "killed"];
    let statCards = statuses.map((stat) => document.querySelector(`#stat-${stat}`));

    fetch("/api/jobs/stats")
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                displayToast(notificationCount, data["message"], currentTime(), "danger")
                notificationCount++;
            } else {
                for (let i = 0; i < statCards.length; i++) {
                    statCards[i].innerHTML = data[statuses[i]];
                }
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, Date.now(), "danger")
            notificationCount++;
        })
}

// Refresh recent jobs of dashboard
function refreshRecentJobs() {
    let recentJobNodes = document.querySelectorAll(".recent-job");
    let jobIds = JSON.stringify(Array.from(recentJobNodes).map(node => node.dataset["id"]));
    const statusColor = {
        "new": "info", "completed": "success", "failed": "danger", "running": "primary",
        "submitted": "secondary", "killed": "warning"
    }

    fetch(`/api/jobs?ids=${jobIds}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                displayToast(notificationCount, data["message"], currentTime(), "danger")
                notificationCount++;
            } else {
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#job-id-${data[i]["id"]}-status`).innerHTML = data[i]["status"]
                    document.querySelector(`#job-id-${data[i]["id"]}-status`).className = `badge badge-${statusColor[data[i]["status"]]} badge-pill`
                }
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, Date.now(), "danger")
            notificationCount++;
        })
}


document.addEventListener('DOMContentLoaded', () => {
    setTimeout(refreshStatistics, 0)
    setTimeout(refreshRecentJobs, 0)
});


// Refresh stat cards of dashboard
function refreshStatistics() {

    // Status category
    let statuses = ["new", "running", "completed", "failed", "killed"];

    // Get status card nodes
    let statCards = statuses.map((stat) => document.querySelector(`#stat-${stat}`));

    // Make ajax request to the server
    fetch("/api/jobs/stats")
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                // If API response false, display error
                displayToast(notificationCount, data["message"], currentTime(), "danger");
                notificationCount++;
            } else {
                // Update stats
                for (let i = 0; i < statCards.length; i++) {
                    statCards[i].innerHTML = data[statuses[i]];
                }
            }
        })
        .catch(err => {
            // Display error if any
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshStatistics, refreshInterval);
}

// Refresh recent jobs of dashboard
function refreshRecentJobs() {

    // Get HTML nodes
    let recentJobNodes = document.querySelectorAll(".recent-job");

    // Extract job ids from the nodes
    let jobIds = JSON.stringify(Array.from(recentJobNodes).map(node => node.dataset["id"]));

    // Make ajax request to the server
    fetch(`/api/jobs?ids=${jobIds}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                // If API response false, display error
                displayToast(notificationCount, data["message"], currentTime(), "danger");
                notificationCount++;
            } else {
                // Update status
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#job-id-${data[i]["id"]}-status`).innerHTML = data[i]["status"];
                    document.querySelector(`#job-id-${data[i]["id"]}-status`).className = `badge badge-${statusColor[data[i]["status"]]} badge-pill`;
                }
            }
        })
        .catch(err => {
            // Display error if any
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshRecentJobs, refreshInterval);
}


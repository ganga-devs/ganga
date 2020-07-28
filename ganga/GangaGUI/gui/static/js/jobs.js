document.addEventListener('DOMContentLoaded', () => {
    setTimeout(refreshJobs, 0)
});


// Refresh jobs
function refreshJobs() {

    // Get HTML node container job id in their dataset
    let jobNodes = document.querySelectorAll(".job");

    // Extract job id from the dataset of the HTML nodes
    let jobIds = JSON.stringify(Array.from(jobNodes).map(node => node.dataset["id"]));

    // Make ajax request to server and update the status of the job
    fetch(`/api/jobs?ids=${jobIds}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                // If server response is false display error
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
            // Display js error, if any
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshJobs, refreshInterval);
}
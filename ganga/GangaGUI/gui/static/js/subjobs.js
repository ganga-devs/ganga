document.addEventListener('DOMContentLoaded', () => {
    setTimeout(refreshSubjobs, 0);
});


// Refresh subjobs
function refreshSubjobs() {

    console.log("Tick")

    // Get HTML nodes containing subjob id in dataset
    let subjobNodes = document.querySelectorAll(".subjob");

    // Extract subjob id from the HTML nodes
    let subjobIds = JSON.stringify(Array.from(subjobNodes).map(node => node.dataset["id"]));

    // Extract job id from the subjob HTML node dataset
    let jobId = document.querySelector(".subjob").dataset["job_id"]

    fetch(`/api/job/${jobId}/subjobs?ids=${subjobIds}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                displayToast(notificationCount, data["message"], currentTime(), "danger");
                notificationCount++;
            } else {
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#subjob-id-${data[i]["id"]}-status`).innerHTML = data[i]["status"];
                    document.querySelector(`#subjob-id-${data[i]["id"]}-status`).className = `badge badge-${statusColor[data[i]["status"]]} badge-pill`;
                }
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        });

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshSubjobs, refreshInterval);
}
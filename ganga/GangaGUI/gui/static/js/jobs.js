document.addEventListener('DOMContentLoaded', () => {
    console.log("It works")
    setInterval(refreshJobs, 2000)
});


// Refresh jobs
function refreshJobs() {

    let jobNodes = document.querySelectorAll(".job");
    let jobIds = JSON.stringify(Array.from(jobNodes).map(node => node.dataset["id"]));

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
                console.log(data)
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
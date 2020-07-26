document.addEventListener('DOMContentLoaded', () => {
    console.log("It works")
    setInterval(refreshSubjobs, 2000)
});


// Refresh subjobs
function refreshSubjobs() {

    let subjobNodes = document.querySelectorAll(".subjob");
    let subjobIds = JSON.stringify(Array.from(subjobNodes).map(node => node.dataset["id"]));

    const statusColor = {
        "new": "info", "completed": "success", "failed": "danger", "running": "primary",
        "submitted": "secondary", "killed": "warning"
    }

    fetch(`/api/jobs?ids=${subjobIds}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                displayToast(notificationCount, data["message"], currentTime(), "danger")
                notificationCount++;
            } else {
                console.log(data)
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#subjob-id-${data[i]["id"]}-status`).innerHTML = data[i]["status"]
                    document.querySelector(`#subjob-id-${data[i]["id"]}-status`).className = `badge badge-${statusColor[data[i]["status"]]} badge-pill`
                }
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, Date.now(), "danger")
            notificationCount++;
        })
}
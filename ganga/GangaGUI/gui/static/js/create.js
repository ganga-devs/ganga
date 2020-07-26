document.addEventListener('DOMContentLoaded', () => {

});

function createJob(template_id) {
    fetch(`/api/job/create`, {
        method: "POST",
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            template_id: template_id
        })
    })
        .then(response => response.json())
        .then(data => {
            displayToast(notificationCount, data["message"], currentTime(), data["success"] ? "success" : "danger")
            notificationCount++;
        })
        .catch(err => {
            displayToast(notificationCount, err, Date.now(), "danger")
            notificationCount++;
        })
    ;
}

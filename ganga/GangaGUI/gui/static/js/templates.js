document.addEventListener('DOMContentLoaded', () => {

});

// Create job - triggered when 'Create Job' button is clicked
function createJob(template_id) {

    // Make ajax request to the server with job id in the body
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
            displayToast(notificationCount, data["message"], currentTime(), data["success"] ? "success" : "danger");
            notificationCount++;
        })
        .catch(err => {
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })
    ;
}

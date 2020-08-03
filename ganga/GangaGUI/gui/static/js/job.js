document.addEventListener('DOMContentLoaded', () => {
    setTimeout(refreshStatus, 0)
});


// Display job attribute info on select
function handleOnSelectAttribute() {
    const jobId = document.querySelector("#job-info").dataset.job_id;
    const attribute = document.querySelector("#attribute-select").value;
    getJobAttributeInfo(jobId, attribute);
}

function getJobAttributeInfo(job_id, attribute) {

    // Make ajax request to server
    fetch(`/api/job/${job_id}/${attribute}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                // If API response false, display error
                displayToast(notificationCount, data["message"], currentTime(), "danger");
                notificationCount++;
            } else {
                // Display attribute info
                const attributeInfoBox = document.querySelector("#attribute-info-box");
                attributeInfoBox.innerHTML = data[attribute];
            }
        })
        .catch(err => {
            // Display error if any
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })
    ;
}

// Execute action on the job
function handleExecuteAction() {
    const jobId = document.querySelector("#job-info").dataset.job_id;
    const action = document.querySelector("#action-select").value;
    let actionData = document.querySelector("#action-data").value;
    actionData = actionData.trim();
    executeAction(jobId, action, actionData);
}

// Submit Job
function handleSubmitJob() {
    const jobId = document.querySelector("#job-info").dataset.job_id;
    executeAction(jobId, "submit", "")
}

// Kill Job
function handleKillJob() {
    const jobId = document.querySelector("#job-info").dataset.job_id;
    executeAction(jobId, "kill", "")
}

// Remove Job
function handleRemoveJob() {
    const jobId = document.querySelector("#job-info").dataset.job_id;
    executeAction(jobId, "remove", "")
}

// Remove Job
function handleCopy() {
    const jobId = document.querySelector("#job-info").dataset.job_id;
    executeAction(jobId, "copy", "")
}

function executeAction(jobId, action, data) {

    const actionData = {}

    // If action data is empty, don't add it to body
    data.length === 0 ? {} : actionData[action] = data;

    // Make ajax request to server
    fetch(`/api/job/${jobId}/${action}`, {
        method: "PUT",
        body: JSON.stringify(actionData)
    })
        .then(response => response.json())
        .then(data => {
            // Display server response
            displayToast(notificationCount, data["message"], currentTime(), data["success"] ? "success" : "danger");
            notificationCount++;
        })
        .catch(err => {
            // Display error if any
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })
    ;
}

// Refresh job status
function refreshStatus() {

    let jobIds = JSON.stringify(Array.from([document.querySelector("#job-info").dataset.job_id]));

    fetch(`/api/jobs?ids=${jobIds}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                displayToast(notificationCount, data["message"], currentTime(), "danger");
                notificationCount++;
            } else {
                document.querySelector(`#job-id-${data[0]["id"]}-status`).innerHTML = data[0]["status"];
                document.querySelector(`#job-id-${data[0]["id"]}-status`).className = `badge badge-${statusColor[data[0]["status"]]} badge-pill ml-2`;

                // Get subjob status card nodes
                let statCards = ["running", "failed-killed", "completing", "completed"].map((stat) => document.querySelector(`#stat-${stat}`));
                let subjobStatuses = data[0]["subjob_statuses"].split("/")
                for (let i = 0; i < statCards.length; i++) {
                    statCards[i].innerHTML = subjobStatuses[i]
                }
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        });

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshStatus, refreshInterval);
}



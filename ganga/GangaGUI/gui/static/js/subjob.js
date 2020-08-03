document.addEventListener('DOMContentLoaded', () => {
    setTimeout(refreshStatus, 0)
});

function handleCopy(jobId, subjobId) {

    // Make ajax request to server
    fetch(`/api/job/${jobId}/subjob/${subjobId}/copy`, {
        method: "PUT"
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

function handleOnSelectAttribute() {
    const jobId = document.querySelector("#subjob-info").dataset.job_id;
    const subjobId = document.querySelector("#subjob-info").dataset.subjob_id;
    const attribute = document.querySelector("#attribute-select").value;
    getSubjobAttributeInfo(jobId, subjobId, attribute);
}

function getSubjobAttributeInfo(job_id, subjob_id, attribute) {

    fetch(`/api/job/${job_id}/subjob/${subjob_id}/${attribute}`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                // If API response false, display error
                displayToast(notificationCount, data["message"], currentTime(), "danger");
                notificationCount++;
            } else {
                const attributeInfoBox = document.querySelector("#attribute-info-box");
                attributeInfoBox.innerHTML = data[attribute];
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })
    ;
}

function refreshStatus() {

    const job_id = document.querySelector("#subjob-info").dataset.job_id;
    const subjob_id = document.querySelector("#subjob-info").dataset.subjob_id;


    fetch(`/api/job/${job_id}/subjob/${subjob_id}/status`)
        .then(response => response.json())
        .then(data => {
            if (data["success"] === false) {
                // If API response false, display error
                displayToast(notificationCount, data["message"], currentTime(), "danger");
                notificationCount++;
            } else {
                document.querySelector(`#subjob-id-${subjob_id}-status`).innerHTML = data["status"];
                document.querySelector(`#subjob-id-${subjob_id}-status`).className = `badge badge-${statusColor[data["status"]]} badge-pill ml-2`;
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, currentTime(), "danger");
            notificationCount++;
        })
    ;

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshStatus, refreshInterval);
}


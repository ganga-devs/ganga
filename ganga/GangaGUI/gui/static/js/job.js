document.addEventListener('DOMContentLoaded', () => {

    // displayToast(1, "Hllo", "test");
    // duplicateToast();
    // handleExecuteAction();
    setInterval(refreshStatus, 2000)
});


function handleOnSelectAttribute() {
    const job_id = document.querySelector("#jobInfo").dataset.job_id;
    const attribute = document.querySelector("#attributeSelect").value;
    console.log(job_id, attribute)
    getJobAttributeInfo(job_id, attribute);
}

function getJobAttributeInfo(job_id, attribute) {
    fetch(`/api/job/${job_id}/${attribute}`)
        .then(response => response.json())
        .then(data => {
            const attributeInfoBox = document.querySelector("#attributeInfoBox");
            attributeInfoBox.innerHTML = data[attribute]
        })
        .catch(err => console.log(err))
    ;
}

function handleExecuteAction() {
    const job_id = document.querySelector("#jobInfo").dataset.job_id;
    const action = document.querySelector("#actionSelect").value
    let actionData = document.querySelector("#actionData").value
    actionData = actionData.trim()
    console.log(job_id, action, actionData)
    executeAction(job_id, action, actionData)
}

function executeAction(job_id, action, data) {

    const actionData = {}

    data.length === 0 ? {} : actionData[action] = data;

    fetch(`/api/job/${job_id}/${action}`, {
        method: "PUT",
        body: JSON.stringify(actionData)
    })
        .then(response => response.json())
        .then(data => {
            let success;
            data["success"] ? success="primary" : success="danger"
            displayToast(notificationCount, data["message"], Date.now(), success)
            notificationCount++;
            console.log(data)
        })
        .catch(err => console.log(err))
    ;
}

function refreshStatus() {

    let jobIds = JSON.stringify(Array.from([document.querySelector("#jobInfo").dataset.job_id]));
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
                document.querySelector(`#job-id-${data[0]["id"]}-status`).innerHTML = data[0]["status"]
                document.querySelector(`#job-id-${data[0]["id"]}-status`).className = `badge badge-${statusColor[data[0]["status"]]} badge-pill ml-2`
            }
        })
        .catch(err => {
            displayToast(notificationCount, err, Date.now(), "danger")
            notificationCount++;
        })
}



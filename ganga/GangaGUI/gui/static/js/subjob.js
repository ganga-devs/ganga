document.addEventListener('DOMContentLoaded', () => {

    setInterval(refreshStatus, 2000)

});

function handleOnSelectAttribute() {
    const job_id = document.querySelector("#subjobInfo").dataset.job_id;
    const subjob_id = document.querySelector("#subjobInfo").dataset.subjob_id;
    const attribute = document.querySelector("#attributeSelect").value;
    console.log(job_id, subjob_id, attribute)
    getSubjobAttributeInfo(job_id, subjob_id, attribute);
}

function getSubjobAttributeInfo(job_id, subjob_id, attribute) {
    fetch(`/api/job/${job_id}/subjob/${subjob_id}/${attribute}`)
        .then(response => response.json())
        .then(data => {
            const attributeInfoBox = document.querySelector("#attributeInfoBox");
            attributeInfoBox.innerHTML = data[attribute]
        })
        .catch(err => {
            displayToast(notificationCount, err, Date.now(), "danger")
            notificationCount++;
        })
    ;
}

function refreshStatus() {

    const job_id = document.querySelector("#subjobInfo").dataset.job_id;
    const subjob_id = document.querySelector("#subjobInfo").dataset.subjob_id;

    const statusColor = {
        "new": "info", "completed": "success", "failed": "danger", "running": "primary",
        "submitted": "secondary", "killed": "warning"
    }

    fetch(`/api/job/${job_id}/subjob/${subjob_id}/status`)
        .then(response => response.json())
        .then(data => {
            document.querySelector(`#subjob-id-${subjob_id}-status`).innerHTML = data["status"]
            document.querySelector(`#subjob-id-${subjob_id}-status`).className = `badge badge-${statusColor[data["status"]]} badge-pill ml-2`
        })
        .catch(err => {
            displayToast(notificationCount, err, Date.now(), "danger")
            notificationCount++;
        })
    ;

}


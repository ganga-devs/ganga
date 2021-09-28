document.addEventListener('DOMContentLoaded', () => {

    // Refresh Job status
    setTimeout(refreshJobStatus, 0);

    // Add event listener to copy button
    const btnCopy = document.querySelector('#btn-copy');
    btnCopy.addEventListener('click', copyHandler);

    // Add event listener to pin button
    const btnPin =  document.querySelector('#btn-pin');
    btnPin.addEventListener('click', pinHandler);

    // Add event listener to submit button
    const btnSubmit =  document.querySelector('#btn-submit');
    btnSubmit.addEventListener('click', submitHandler);

    // Add event listener to kill button
    const btnKill =  document.querySelector('#btn-kill');
    btnKill.addEventListener('click', killHandler);

    // Add event listener to freeze button
    const btnFreeze = document.querySelector('#btn-freeze');
    btnFreeze.addEventListener('click', freezeHandler);

    // Add event listener to unfreeze button
    const btnUnFreeze = document.querySelector('#btn-unfreeze');
    btnUnFreeze.addEventListener('click', unfreezeHandler);

    // Add event listener to remove button
    const btnRemove =  document.querySelector('#btn-remove');
    btnRemove.addEventListener('click', removeHandler);

    // Add event listener to display attribute information
    const attributeSelect =  document.querySelector('#attribute-select');
    attributeSelect.addEventListener('change', attributeSelectHandler);

    // Add event listener to display attribute information
    const btnExecute =  document.querySelector('#btn-execute');
    btnExecute.addEventListener('click', executeHandler);

});


// Handles copying of the job
function copyHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Make ajax request to server
    fetch(`/api/jobs/${jobId}/copy`, {
        method: 'PUT',
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            // Display server response
            displayToast(data['message'], data['success'] ? 'success' : 'danger');

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })
    ;

}


// Handles pinning of the job
function pinHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Make ajax request to server
    fetch(`/api/jobs/${jobId}/pin`, {
        method: 'PUT',
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            // Display server response
            displayToast(data['message'], data['success'] ? 'success' : 'danger');

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })
    ;

}


// Handles submitting of the job
function submitHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Execute submit action
    executeAction(jobId, 'submit');

}


// Handles killing of the job
function killHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Execute kill action
    executeAction(jobId, 'kill');

}

// Handles freezing of the job
function freezeHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Execute freeze action
    executeAction(jobId, 'freeze');

}

// Handles unfreezing of the job
function unfreezeHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Execute unfreeze action
    executeAction(jobId, 'unfreeze');

}


// Handles removing of the job
function removeHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Make ajax request to server to remove the job
    fetch(`/api/jobs/${jobId}`, {
        method: "DELETE",
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            if (data['success'] === false) {

                // If API response false, display error
                displayToast(data['message'], 'danger');

            } else {

                // Display remove message
                displayToast(data['message'], 'success');

                console.log("Navigating 4...")

                // Change page after 3 seconds
                setTimeout(() => window.location.href = document.referrer, 2000)

            }

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })
    ;

    // Execute remove action
    // executeAction(jobId, 'remove');

}


// Handles displaying of attribute information of the job
function attributeSelectHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Get attribute to query
    const attribute = e.target.value

    // Make ajax request to server
    fetch(`/api/jobs/${jobId}/${attribute}`, {
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            if (data['success'] === false) {

                // If API response false, display error
                displayToast(data['message'], 'danger');

            } else {

                // Display attribute info
                const attributeInfoBox = document.querySelector('#attribute-info-box');
                attributeInfoBox.innerHTML = data[attribute];

            }

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })
    ;

}


// Handles execution of action on the job
function executeHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Job id
    const jobId = e.target.dataset.id;

    // Action to execute
    const action = document.querySelector('#action-select').value;

    // Action argument
    const actionArgument = document.querySelector('#action-argument').value;

    // Execute action
    executeAction(jobId, action, actionArgument.trim());

}


// Execute action on the job
function executeAction(jobId, action, argument='') {

    const actionArgument = {};

    // If action data is empty, don't add it to body
    if (argument.length !== 0) {
        actionArgument[action] = argument;
    }

    // Make ajax request to server
    fetch(`/api/jobs/${jobId}/${action}`, {
        method: 'PUT',
        body: JSON.stringify(actionArgument),
        headers: {
            'Content-Type': 'application/json',
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            // Display server response
            displayToast(data['message'],data['success'] ? 'success' : 'danger');

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })
    ;

}


// Refresh job status
function refreshJobStatus() {

    // Job id
    const jobId = document.querySelector('#job-info').dataset.job_id;

    // Make request to server for job information
    fetch(`/api/jobs/${jobId}`, {
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            if (data['success'] === false) {

                // If server responds with failure, display message
                displayToast(data['message'], 'danger');

            } else {

                // Get locations to update
                document.querySelector(`#job-id-${data['id']}-status`).innerHTML = data['status'];
                document.querySelector(`#job-id-${data['id']}-status`).className = `badge badge-${statusColor[data['status']]} badge-pill ml-2`;

                // Get subjob status card nodes
                const statCards = ['running', 'failed-killed', 'completing', 'completed'].map((stat) => document.querySelector(`#stat-${stat}`));
                let subjobStatuses = data['subjob_statuses'].split('/')
                for (let i = 0; i < statCards.length; i++) {
                    statCards[i].innerHTML = subjobStatuses[i]
                }

            }

        })
        .catch(err => {

            // Display error, if any
            displayToast(err, 'danger');

        });

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshJobStatus, refreshInterval);

}



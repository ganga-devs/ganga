document.addEventListener('DOMContentLoaded', () => {

    // Refresh subjob status
    setTimeout(refreshStatus, 0)

    // Add event listener to copy button
    const btnCopy = document.querySelector('#btn-copy');
    btnCopy.addEventListener('click', copyHandler);

    // Add event listener to display attribute information
    const attributeSelect =  document.querySelector('#attribute-select');
    attributeSelect.addEventListener('change', attributeSelectHandler);

});


// Handles copying of the subjob to a new job
function copyHandler(e) {

    // Prevent default behavior
    e.preventDefault();

    // Get job id from the btn dataset
    const jobId = e.target.dataset.job_id;

    // Get subjob id from the btn dataset
    const subjobId = e.target.dataset.subjob_id;

    // Make ajax request to server
    fetch(`/api/jobs/${jobId}/subjobs/${subjobId}/copy`, {
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


// Handles displaying attribute information of the subjob
function attributeSelectHandler(e) {

    // Get job id value from the select dataset
    const jobId = e.target.dataset.job_id;

    // Get the subjob id value from the select dataset
    const subjobId = e.target.dataset.subjob_id;

    // Get attribute value from the select value
    const attribute = e.target.value;

    // Make a request to the server
    fetch(`/api/jobs/${jobId}/subjobs/${subjobId}/${attribute}`, {
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

                // Display attribute information
                const attributeInfoBox = document.querySelector('#attribute-info-box');
                attributeInfoBox.innerHTML = data[attribute];

            }

        })
        .catch(err => {

            // Display error while making the request, if any
            displayToast(err, 'danger');

        })
    ;
}


// Refreshes status of the job
function refreshStatus() {

    // Job id and subjob id
    const jobId = document.querySelector('#subjob-info').dataset.job_id;
    const subjobId = document.querySelector('#subjob-info').dataset.subjob_id;

    // Get subjob information from the server
    fetch(`/api/jobs/${jobId}/subjobs/${subjobId}/status`, {
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

                // Update status
                document.querySelector(`#subjob-id-${subjobId}-status`).innerHTML = data['status'];
                document.querySelector(`#subjob-id-${subjobId}-status`).className = `badge badge-${statusColor[data['status']]} badge-pill ml-2`;

            }

        })
        .catch(err => {

            // Display error while making the request, if any
            displayToast(err, 'danger');

        })
    ;

    // Recursively refresh
    const refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshStatus, refreshInterval);

}


document.addEventListener('DOMContentLoaded', () => {

    // Refresh subjobs statuses
    setTimeout(refreshSubjobs, 0);

});


// Refresh subjobs
function refreshSubjobs() {

    // Get HTML nodes containing subjob id in dataset
    const subjobNodes = document.querySelectorAll('.subjob');

    // Extract subjob id from the HTML nodes
    const subjobIds = JSON.stringify(Array.from(subjobNodes).map(node => node.dataset['id']));

    // Extract job id from the subjob HTML node dataset
    const jobId = document.querySelector('.subjob').dataset['job_id']

    // Make request to the server
    fetch(`/api/jobs/${jobId}/subjobs?ids=${subjobIds}`, {
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            if (data['success'] === false) {

                // If server responds to failure, display the error message
                displayToast(data['message'], 'danger');

            } else {

                // Refresh statuses
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#subjob-id-${data[i]['id']}-status`).innerHTML = data[i]['status'];
                    document.querySelector(`#subjob-id-${data[i]['id']}-status`).className = `badge badge-${statusColor[data[i]['status']]} badge-pill`;
                }

            }

        })
        .catch(err => {

            // Display error, if any
            displayToast(err, "danger");

        });

    // Recursively refresh
    const refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshSubjobs, refreshInterval);

}
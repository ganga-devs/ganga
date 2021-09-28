
document.addEventListener('DOMContentLoaded', () => {

    // Refresh jobs statuses
    setTimeout(refreshJobs, 0)

});


// Refresh jobs
function refreshJobs() {

    // Get HTML node containing job id in their dataset
    const jobNodes = document.querySelectorAll('.job');

    // Extract job id from the dataset of the HTML nodes
    const jobIds = JSON.stringify(Array.from(jobNodes).map(node => node.dataset['id']));

    // Make ajax request to server and update the status of the job
    fetch(`/api/jobs?ids=${jobIds}`, {
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            if (data['success'] === false) {

                // If server response is false display error
                displayToast(data['message'],  'danger');

            } else {

                // Update status
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#job-id-${data[i]['id']}-status`).innerHTML = data[i]['status'];
                    document.querySelector(`#job-id-${data[i]['id']}-status`).className = `badge badge-${statusColor[data[i]['status']]} badge-pill`;
                }

            }

        })
        .catch(err => {

            // Display js error, if any
            displayToast(err, "danger");

        })

    // Recursively refresh
    const refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshJobs, refreshInterval);

}

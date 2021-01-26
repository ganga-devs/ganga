document.addEventListener('DOMContentLoaded', () => {

    // Refresh initial stats and statuses
    setTimeout(refreshStatistics, 0)
    setTimeout(refreshRecentJobs, 0)
    setTimeout(refreshPinnedJobs, 0)

    // Add event listeners to copy button
    const btnCopyList = document.querySelectorAll('.btn-copy')
    Array.from(btnCopyList).map(btnCopy => btnCopy.addEventListener('click', copyHandler))

    // Add event listeners to unpin button
    const btnUnpinList = document.querySelectorAll('.btn-unpin')
    Array.from(btnUnpinList).map(btnUnpin => btnUnpin.addEventListener('click', unpinHandler))

});


// Handles copy of the job
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


// Handles unpin of the job
function unpinHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get job id from the copy button dataset
    const jobId = e.target.dataset.id;

    // Make ajax request to server
    fetch(`/api/jobs/${jobId}/unpin`, {
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


// Refresh stat cards of dashboard
function refreshStatistics() {

    // Status category
    const statuses = ['new', 'running', 'completed', 'failed', 'killed'];

    // Get status card nodes
    const statCards = statuses.map((stat) => document.querySelector(`#stat-${stat}`));

    // Make ajax request to the server
    fetch('/api/jobs/statistics', {
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

                // Update stats
                for (let i = 0; i < statCards.length; i++) {
                    statCards[i].innerHTML = data[statuses[i]];
                }

            }

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })

    // Recursively refresh
    const refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshStatistics, refreshInterval);
}


// Refresh recent jobs of dashboard
function refreshRecentJobs() {

    // Get HTML nodes
    let recentJobNodes = document.querySelectorAll('.recent-job');

    // Extract job ids from the nodes
    let jobIds = JSON.stringify(Array.from(recentJobNodes).map(node => node.dataset['id']));

    // Make ajax request to the server
    fetch(`/api/jobs?ids=${jobIds}`, {
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
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#job-id-${data[i]['id']}-status`).innerHTML = data[i]['status'];
                    document.querySelector(`#job-id-${data[i]['id']}-status`).className = `badge badge-${statusColor[data[i]['status']]} badge-pill`;
                    document.querySelector(`#subjob-statuses-job-id-${data[i]['id']}`).innerHTML = 'subjobs statuses: ' + data[i]['subjob_statuses'];

                }

            }

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshRecentJobs, refreshInterval);
}


// Refresh pinned jobs
function refreshPinnedJobs() {

    // Get HTML nodes
    let pinnedJobNodes = document.querySelectorAll('.pinned-job');

    // Extract job ids from the nodes
    let jobIds = JSON.stringify(Array.from(pinnedJobNodes).map(node => node.dataset['id']));

    // Make ajax request to the server
    fetch(`/api/jobs?ids=${jobIds}`, {
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
                for (let i = 0; i < data.length; i++) {
                    document.querySelector(`#pinned-job-id-${data[i]['id']}-status`).innerHTML = data[i]['status'];
                    document.querySelector(`#pinned-job-id-${data[i]['id']}-status`).className = `badge badge-${statusColor[data[i]['status']]} badge-pill`;
                    document.querySelector(`#subjob-statuses-pinned-job-id-${data[i]['id']}`).innerHTML = 'subjobs statuses: ' + data[i]['subjob_statuses'];

                }

            }

        })
        .catch(err => {

            // Display error if any
            displayToast(err, 'danger');

        })

    // Recursively refresh
    let refreshInterval = Number(localStorage.getItem('refreshInterval')) || 5000;
    setTimeout(refreshPinnedJobs, refreshInterval);
}


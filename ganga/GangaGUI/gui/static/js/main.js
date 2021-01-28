document.addEventListener('DOMContentLoaded', () => {

    // Set the refresh interval selector value to the active value
    document.querySelector('#refresh-interval').value = localStorage.getItem('refreshInterval');

    // Add event listener to refresh interval selector in the footer
    const refreshIntervalSelector = document.querySelector('#refresh-interval');
    refreshIntervalSelector.addEventListener('change', updateRefreshInterval);

    // Add event listen to logout button to trigger logout function
    const btnLogout = document.querySelector('#btn-logout');
    btnLogout.addEventListener('click', logoutHandler);

});


// Global variables
let notificationCount = 0;
const statusColor = {
    'new': 'info',
    'completed': 'success',
    'completed_frozen': 'success',
    'failed': 'danger',
    'failed_frozen': 'danger',
    'running': 'primary',
    'submitted': 'secondary',
    'killed': 'warning'
};


// Returns time in readable format
function currentTime() {
    const date = new Date();
    return 'Timestamp ' + date.getHours() + ":" + date.getMinutes();
}


// Display notification
function displayToast(message, type, id=notificationCount, time=currentTime()) {

    // Notification template based on Bootstrap CSS Toast
    const bootstrapToast = `
        <div class='toast' data-delay='10000' id='notification-{{id}}' role='alert' aria-live='assertive' aria-atomic='true'>
            <div class='toast-header d-flex'>
                <strong class='flex-grow-1 text-{{type}}'>Notification</strong>
                <small class='text-muted mx-3'>{{ time }}</small>
                <button type='button' class='ml-2 mb-1 close' data-dismiss='toast' aria-label='Close'>
                    <span aria-hidden='true'>&times;</span>
                </button>
            </div>
            <div class='toast-body'>
                {{message}}
            </div>
        </div>
    `;

    // Compile the template
    const template = Handlebars.compile(bootstrapToast);
    const toastContainer = document.querySelector('#toast-container');
    toastContainer.insertAdjacentHTML('beforeend', template({message: message, time: time, id: id, type: type}));
    $(`#notification-${id}`).toast('show');
    notificationCount++;

}


// Handle update of refresh interval time
function updateRefreshInterval(e) {
    let refreshInterval = e.target.value;
    localStorage.setItem('refreshInterval', refreshInterval);
}


// Handle logout
function logoutHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Remove token from the browser storage
    localStorage.removeItem('token');

    // Navigate to logout route
    window.location.href = '/logout';

}

document.addEventListener('DOMContentLoaded', () => {
    document.querySelector("#refresh-interval").value = localStorage.getItem('refreshInterval');
});

let notificationCount = 0

const statusColor = {
        "new": "info", "completed": "success", "failed": "danger", "running": "primary",
        "submitted": "secondary", "killed": "warning"
    }

function currentTime() {
    let date = new Date();
    return "Timestamp " + date.getHours() + ":" + date.getMinutes();
}

function displayToast(id, message, time, type) {

    const bootstrapToast = `
        <div class="toast" data-delay="10000" id="notification{{id}}" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="toast-header d-flex">
                <strong class="flex-grow-1 text-{{type}}">Notification</strong>
                <small class="text-muted mx-3">{{ time }}</small>
                <button type="button" class="ml-2 mb-1 close" data-dismiss="toast" aria-label="Close">
                    <span aria-hidden="true">&times;</span>
                </button>
            </div>
            <div class="toast-body">
                {{message}}
            </div>
        </div>
    `;

    // compile the template
    const template = Handlebars.compile(bootstrapToast);
    let toastContainer = document.querySelector("#toastContainer");
    console.log(toastContainer)
    toastContainer.insertAdjacentHTML("beforeend", template({message: message, time: time, id: id, type: type}))
    $(`#notification${id}`).toast('show')
}

function updateRefreshInterval() {
    let refreshInterval = document.querySelector("#refresh-interval").value;
    console.log(refreshInterval);
    localStorage.setItem('refreshInterval', refreshInterval);
}
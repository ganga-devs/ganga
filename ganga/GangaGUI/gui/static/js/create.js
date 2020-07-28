document.addEventListener('DOMContentLoaded', () => {

});

function createJob(template_id) {

    // Make ajax request to the server with template id in the body
    fetch(`/api/job/create`, {
        method: "POST",
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            template_id: template_id
        })
    })
        .then(response => response.json())
        .then(data => {
            // Display server response
            displayToast(notificationCount, data["message"], currentTime(), data["success"] ? "success" : "danger");
            notificationCount++;
        })
        .catch(err => {
            // Display error if any
            displayToast(notificationCount, err, currentTime(), "danger")
            notificationCount++;
        })
    ;
}


// For updating the loadfile label to the filename of selected file
function updateLoadfileFilename() {
    let loadfileInput = document.querySelector("#loadfile-input");
    let loadfileLabel = document.querySelector("#loadfile-input-label");
    loadfileLabel.innerHTML = loadfileInput.files[0].name;
}


// For updating the runfile label to the filename of selected file
function updateRunfileFilename() {
    let runfileInput = document.querySelector("#runfile-input");
    let runfileLabel = document.querySelector("#runfile-input-label");
    runfileLabel.innerHTML = runfileInput.files[0].name;
}
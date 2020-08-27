document.addEventListener('DOMContentLoaded', () => {

    // Add event listener to update the loadfile label to the selected file
    loadfileInput = document.querySelector('#loadfile-input');
    loadfileInput.addEventListener('change', updateLoadfileFilename);

    // Add event listener to update the runfile label to the selected file
    runfileInput = document.querySelector('#runfile-input');
    runfileInput.addEventListener('change', updateRunfileFilename);

    // Add event listeners to create buttons
    const btnCreateList = document.querySelectorAll('.btn-create');
    Array.from(btnCreateList).map(btnCreate => btnCreate.addEventListener('click', createJobHandler));

});

function createJobHandler(e) {

    // Get template id from the dataset of button
    const templateId = e.target.dataset.id;

    // Make ajax request to the server with template id in the body
    fetch(`/api/jobs/create`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-Access-Token': localStorage.getItem('token')
        },
        body: JSON.stringify({
            template_id: templateId
        })
    })
        .then(response => response.json())
        .then(data => {

            // Display server response
            displayToast(data['message'], data['success'] ? 'success' : 'danger');

        })
        .catch(err => {

            // Display error if any
            displayToast(err, "danger");

        })
    ;

}


// For updating the loadfile label to the filename of selected file
function updateLoadfileFilename(e) {

    // loadfile Input
    const loadfileInput = e.target;

    // Label to be updated
    const loadfileLabel = document.querySelector("#loadfile-input-label");

    // Update label
    loadfileLabel.innerHTML = loadfileInput.files[0].name;
}


// For updating the runfile label to the filename of selected file
function updateRunfileFilename(e) {

    // runfile Input
    const runfileInput = e.target;

    // Label to be update
    const runfileLabel = document.querySelector("#runfile-input-label");

    // Update label
    runfileLabel.innerHTML = runfileInput.files[0].name;
}
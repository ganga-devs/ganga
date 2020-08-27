document.addEventListener('DOMContentLoaded', () => {

    // Add event listeners to create buttons
    const btnCreateList = document.querySelectorAll('.btn-create');
    Array.from(btnCreateList).map(btnCreate => btnCreate.addEventListener('click', createJobHandler));

    // Add event listeners to delete buttons
    const btnTemplateInfoList = document.querySelectorAll('.btn-template-info');
    Array.from(btnTemplateInfoList).map(btnTemplateInfo => btnTemplateInfo.addEventListener('click', templateInfoHandler));

    // Add event listeners to delete buttons
    const btnDeleteList = document.querySelectorAll('.btn-delete');
    Array.from(btnDeleteList).map(btnDelete => btnDelete.addEventListener('click', deleteTemplateHandler));

});


// Make an request to create a job using template
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

// Display template information
function templateInfoHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get template id from the dataset of button
    const templateId = e.target.dataset.id;

    // Make ajax request to the server to get the template full print
    fetch(`/api/templates/${templateId}/full-print`, {
        method: 'GET',
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

                // Display toast
                const templateInfoBox = document.querySelector("#template-info-box");
                templateInfoBox.innerHTML = data;
                $('#template-info-modal').modal('show')

            }

        })
        .catch(err => {

            // Display error if any
            displayToast(err, "danger");

        })
    ;

}


// Make an request to delete the template
function deleteTemplateHandler(e) {

    // Prevent default behaviour
    e.preventDefault();

    // Get template id from the dataset of button
    const templateId = e.target.dataset.id;

    // Make ajax request to the server to delete the template
    fetch(`/api/templates/${templateId}`, {
        method: 'DELETE',
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

                // Display success message
                displayToast(data['message'],  'success');

                // Delete row
                const templateRow = document.querySelector(`#template-${templateId}`);
                templateRow.remove();

            }

        })
        .catch(err => {

            // Display error if any
            displayToast(err, "danger");

        })
    ;

}



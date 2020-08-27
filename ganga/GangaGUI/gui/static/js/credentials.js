document.addEventListener('DOMContentLoaded', () => {

    // Add event listener to renew button
    const btnRenew = document.querySelector('#btn-renew')
    btnRenew.addEventListener('click', renewCredentials)

});

// Renew Credentials
function renewCredentials() {

    // Make ajax request to the server to renew credentials
    fetch(`/api/credentials/renew`, {
        method: 'PUT',
        headers: {
            'X-Access-Token': localStorage.getItem('token')
        }
    })
        .then(response => response.json())
        .then(data => {

            // Display server response
            displayToast(data["message"], data["success"] ? "success" : "danger");

        })
        .catch(err => {

            // Display error if any
            displayToast(err, "danger")

        })
    ;

}
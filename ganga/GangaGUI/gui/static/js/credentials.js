document.addEventListener('DOMContentLoaded', () => {

});

// Renew Credentials
function renewCredentials() {

    // Make ajax request to the server with template id in the body
    fetch(`/api/credential_store/renew`, {
        method: "PUT",
        headers: {

        },
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
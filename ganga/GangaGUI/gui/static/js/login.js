document.addEventListener('DOMContentLoaded', () => {

    // Login handler
    const btnSubmit = document.querySelector('#btn-submit');
    btnSubmit.addEventListener('click', loginHandler);

});


function loginHandler(e) {

    // Prevent form from submitting
    e.preventDefault();

    // Get form username and password
    const username = document.querySelector('#username').value;
    const password = document.querySelector('#password').value;

    // For displaying any error
    const errorNode = document.querySelector('#error');

    // Clear exisiting errors
    errorNode.innerHTML = '';

    // Convert the username and password into JSON
    const jsonBody = JSON.stringify({
        username: username,
        password: password
    });

    // Make server request to get token
    fetch('/token', {
        method: 'post',
        headers: {
            'Content-Type': 'application/json'
        },
        body: jsonBody,
    })
        .then(response => response.json())
        .then(data => {

            if (data['success'] === false) {
                // If API responds with failure, display the error message
                errorNode.innerHTML = data['message'];
            } else {
                // Extract token from the data and store it
                const token = data['token'];
                if (token !== undefined) {
                    localStorage.setItem('token', token);

                    // After storing the token, submit the form
                    loginForm = document.querySelector('#login-form');
                    loginForm.submit();
                } else {
                    errorNode.innerHTML = 'Token not found!';
                }
            }

        })
        .catch(err => {
            // Display error, if any
            errorNode.innerHTML = err;
        })

}
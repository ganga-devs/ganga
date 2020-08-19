document.addEventListener('DOMContentLoaded', () => {

    // Set src of Iframe for accessing Web CLI
    const cliContainer = document.querySelector('#cli-container');
    const cliIframe = document.querySelector('#cli-iframe');
    cliIframe.src = `${location.protocol}//${document.domain}:${cliContainer.dataset.port}/cli`;

    // Add event listener to button to toggle web cli
    const btnCli = document.querySelector('#btn-cli');
    btnCli.addEventListener('click', toggleCli)

});


// Show/Hide CLI
let cliOpen = false;
function toggleCli() {
    document.querySelector('#cli-container').style['display'] = cliOpen ? 'none' : 'initial';
    cliOpen = !cliOpen;
}

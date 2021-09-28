document.addEventListener('DOMContentLoaded', () => {

    // Add event listener to button to toggle web cli
    const btnCliList = document.querySelectorAll('.btn-cli');
    Array.from(btnCliList).map(btnCli => btnCli.addEventListener('click', toggleCli));


});


// Show/Hide CLI
let cliOpen = false;
function toggleCli() {
    document.querySelector('#cli-container').style['display'] = cliOpen ? 'none' : 'initial';
    cliOpen = !cliOpen;
}

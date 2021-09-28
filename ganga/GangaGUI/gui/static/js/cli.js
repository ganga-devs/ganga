
// Apply XtermJS Addons
Terminal.applyAddon(fullscreen)
Terminal.applyAddon(fit)
Terminal.applyAddon(webLinks)
Terminal.applyAddon(search)

// Create new xtermjs terminal
const term = new Terminal({
    cursorBlink: true,
    macOptionIsMeta: true,
    scrollback: true,
});

term.open(document.getElementById('terminal'));

term.fit()
term.resize(40, 50)

// term.toggleFullScreen(true)
term.fit()

term.write("Welcome to Ganga CLI - Ganga GUI\n")

term.on('key', (key, ev) => {
    console.log("pressed key", key)
    console.log("event", ev)
    socket.emit("pty-input", {"input": key})
});

const socket = io.connect('/pty')

const status = document.getElementById("status")
const statusBar = document.querySelector(".status-bar")

socket.on("pty-output", function (data) {
    console.log("new output", data)
    term.write(data.output)
})

socket.on("connect", () => {
        fitToScreen()
        status.innerHTML = 'Connected';
        statusBar.className = "status-bar connected";
    }
)

socket.on("disconnect", () => {
    status.innerHTML = 'Disconnected';
    statusBar.className = "status-bar disconnected";
})

function fitToScreen() {
    term.fit()
    socket.emit("resize", {"cols": term.cols, "rows": term.rows})
}

function debounce(func, wait_ms) {
    let timeout
    return function (...args) {
        const context = this
        clearTimeout(timeout)
        timeout = setTimeout(() => func.apply(context, args), wait_ms)
    }
}

const wait_ms = 50;
window.onresize = debounce(fitToScreen, wait_ms)

function clear() {
    term.clear();
}
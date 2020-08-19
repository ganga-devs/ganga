#!/usr/bin/env python3

from GangaGUI.start import start_gui_server, create_default_user
from GangaGUI.web_cli.web_cli import start_web_cli

# Defaults
web_cli_host = "0.0.0.0"
web_cli_port = 5600
gui_host = "0.0.0.0"
gui_port = 5500
internal_server_port = 5000

# Create default user
gui_user, gui_password = create_default_user()

# Start the GUI Server on a gunicorn server
gui_server = start_gui_server(gui_host=gui_host, gui_port=gui_port, internal_port=internal_server_port, web_cli_mode=True, web_cli_port=web_cli_port)

print(f"GUI Server has been start with PID: {gui_server.pid}")
print(f"You can now access the GUI at: http://{gui_host}:{gui_port}")
print(f"You login information for the GUI is: Username: {gui_user.user} Password: {gui_password}")

# Start the web cli - this will also start a Ganga Session
start_web_cli(host=web_cli_host, port=web_cli_port, internal_port=internal_server_port)

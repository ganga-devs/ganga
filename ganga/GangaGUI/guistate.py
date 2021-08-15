# Global API Server, will run in Ganga thread pool
api_server = None

# Global GUI Server, will start gunicorn server using subprocess.popen
gui_server = None

def set_api_server(value):
    global api_server
    api_server = value

def get_api_server():
    return api_server

def set_gui_server(value):
    global gui_server
    gui_server = value

def get_gui_server():
    return gui_server

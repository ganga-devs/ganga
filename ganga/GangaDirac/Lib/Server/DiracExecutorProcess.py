import functools
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Process


class DiracProcess(Process):
    def __init__(self, task_queue, task_result_dict, env=None):
        super(Process, self).__init__()
        self.daemon = True
        self.task_queue = task_queue
        self.task_result_dict = task_result_dict
        self.env = env
        paths = ['/root/ganga/ganga', '/root/ganga/ganga', '/root/ganga/ganga', '/root/ganga/ganga', '/external/google-api-python-client/1.1/noarch/python', '/external/python-gflags/2.0/noarch/python', '/external/httplib2/0.8/noarch/python', '/external/ipython/1.2.1/noarch/lib/python', '/root/ganga/bin/ganga', '/root/ganga/ganga/ganga', '/root/ganga/ganga', '/root/ganga/bin',
                 '/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.4.21-1666595150/Linux-x86_64/lib/python39.zip', '/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.4.21-1666595150/Linux-x86_64/lib/python3.9', '/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.4.21-1666595150/Linux-x86_64/lib/python3.9/lib-dynload', '/root/.local/lib/python3.9/site-packages', '/root/ganga', '/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.4.21-1666595150/Linux-x86_64/lib/python3.9/site-packages']
        for path in paths:
            if path not in sys.path:
                sys.path.insert(1, path)
        sys.base_exec_prefix = '/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.4.21-1666595150/Linux-x86_64'
        sys.base_prefix = '/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.4.21-1666595150/Linux-x86_64'

    def initialize_dirac_api(self):
        with open('new-diraclog.log', 'a') as f:
            f.write('Attempting to intialize DIRAC\n')
            try:
                from DIRAC.Core.Base.Script import parseCommandLine  # type: ignore
                parseCommandLine(ignoreErrors=False)
                f.write('DIRAC Initialized')
            except Exception as e:
                f.write('DIRAC initialization failed: \n')
                f.write(f"{str(e)}\n")

    def run(self):
        def send_result(event, id, lock, future):
            with lock:
                self.task_result_dict[id] = future.result()
                event.set()

        with open('new-diraclog.log', 'w') as f:
            f.write('Attempting to set environment\n')
            if self.env:
                try:
                    for var_name, value in self.env.items():
                        os.environ[var_name.decode()] = value.decode()
                except Exception as e:
                    f.write('Environment setting failed: \n')
                    f.write(f"{str(e)}\n")

            with open('new-diraclog.log', 'w') as f:
                f.write(f"CURRENT PID: {os.getpid()}\n")
                f.write('TARGET ENV:\n')
                f.write(str(self.env))
                f.write('\nCURRENT PATH:\n')
                f.write(os.getenv('PATH'))
                f.write("\n")

            self.initialize_dirac_api()

            executor = ThreadPoolExecutor()
            lock = Lock()
            while True:
                is_done, task_id, cmd, args_dict = self.task_queue.get()
                future = executor.submit(self.run_dirac_command, cmd, args_dict)
                future.add_done_callback(functools.partial(send_result, is_done, task_id, lock))

    def run_dirac_command(self, cmd, args_dict):
        from DIRAC.Interfaces.API.Dirac import Dirac  # type: ignore
        dirac = Dirac()
        args_dict['dirac'] = dirac
        return_value = cmd(**args_dict)
        return return_value

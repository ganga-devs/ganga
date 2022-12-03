import functools
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Process

from GangaCore.Core.exceptions import TerminationSignalException


class DiracProcess(Process):
    def __init__(self, task_queue, task_result_dict, env=None):
        super(Process, self).__init__()
        self.daemon = True
        self.task_queue = task_queue
        self.task_result_dict = task_result_dict
        self.env = env

    def set_process_env(self):
        if self.env:
            # Convert dict of bytes to strings if necessary
            for key, val in self.env.copy().items():
                try:
                    self.env[key.decode()] = val.decode()
                    del self.env[key]
                except AttributeError:
                    pass

            for var_name, value in self.env.items():
                os.environ[var_name] = value

            if self.env['DIRACOS']:
                dirac_location = self.env['DIRACOS']
                sys.base_exec_prefix = dirac_location
                sys.base_prefix = dirac_location
                paths = [f'{dirac_location}/lib/python39.zip',
                         f'{dirac_location}/lib/python3.9',
                         f'{dirac_location}/lib/python3.9/lib-dynload',
                         f'{dirac_location}/lib/python3.9/site-packages']
                for path in paths:
                    if path not in sys.path:
                        sys.path.insert(1, path)

    def initialize_dirac_api(self):
        from DIRAC.Core.Base.Script import parseCommandLine  # type: ignore
        parseCommandLine(ignoreErrors=False)

    def handle_termination(self):
        self.executor.shutdown()
        sys.exit(0)

    def run(self):
        def send_result(event, id, lock, future):
            with lock:
                self.task_result_dict[id] = future.result()
                event.set()

        self.set_process_env()
        self.initialize_dirac_api()
        self.executor = ThreadPoolExecutor()
        lock = Lock()
        while True:
            try:
                is_done, task_id, cmd, args_dict = self.task_queue.get()
                future = self.executor.submit(self.run_dirac_command, cmd, args_dict)
                future.add_done_callback(functools.partial(send_result, is_done, task_id, lock))
            except TerminationSignalException:
                self.handle_termination()

    def run_dirac_command(self, cmd, args_dict):
        from DIRAC.Interfaces.API.Dirac import Dirac  # type: ignore
        dirac = Dirac()
        args_dict['dirac'] = dirac
        return_value = cmd(**args_dict)
        return return_value

import functools
import os
from queue import Empty
import sys
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Process
import time
import traceback


class DiracProcess(Process):
    def __init__(self, task_queue, task_result_dict, stop_event, env=None):
        super(Process, self).__init__()
        self.daemon = True
        self.task_queue = task_queue
        self.stop_event = stop_event
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
        while not self.stop_event.is_set():
            try:
                task_id, cmd, args_dict = self.task_queue.get_nowait()
                future = self.executor.submit(self.run_dirac_command, cmd, args_dict)
                future.add_done_callback(functools.partial(send_result, task_id, lock))
            except Empty:
                time.sleep(0.05)
                pass
            except Exception:
                self.logger.exception(traceback.format_exc())

        self.handle_termination()()

    def run_dirac_command(self, cmd, args_dict):
        from DIRAC.Interfaces.API.Dirac import Dirac  # type: ignore
        dirac = Dirac()
        args_dict['dirac'] = dirac
        return_value = cmd(**args_dict)
        return return_value

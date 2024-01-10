import functools
import logging
import os
from queue import Empty
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process
import time
import traceback
from UltraDict import UltraDict


class DiracProcess(Process):
    def __init__(self, task_queue, task_result_dict_name, stop_event, logger, env=None):
        super(Process, self).__init__()
        self.daemon = True
        self.task_queue = task_queue
        self.stop_event = stop_event
        self.logger = logger
        self.task_result_dict = UltraDict(name=task_result_dict_name, auto_unlink=True)
        self.env = env
        sys.stdout = LoggerWriter(logger, logging.DEBUG)
        sys.stderr = LoggerWriter(logger, logging.WARN)

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
                paths = self.get_dirac_cvmfs_path()
                for path in paths:
                    if path not in sys.path:
                        sys.path.insert(1, path)

    def initialize_dirac_api(self):
        try:
            from DIRAC.Core.Base.Script import parseCommandLine  # type: ignore
            parseCommandLine(ignoreErrors=False)
        except Exception:
            self.logger.exception(traceback.format_exc())

    def get_dirac_cvmfs_path(self):
        diracos = self.env['DIRACOS']
        python_version_regex = re.compile('python[2-3].[0-9]+$')
        for name in os.listdir(f"{diracos}/lib"):
            if python_version_regex.match(name) and name != 'python3.1':
                python_version = name
        paths = [f'{diracos}/lib/{python_version.replace(".", "")}.zip',
                f'{diracos}/lib/{python_version}',
                f'{diracos}/lib/{python_version}/lib-dynload',
                f'{diracos}/lib/{python_version}/site-packages']
        return paths

    def handle_termination(self):
        try:
            self.executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            self.logger.exception(traceback.format_exc())
        sys.exit(0)

    def run(self):

        def send_result(id, future):
            with self.task_result_dict.lock:
                if future.cancelled():
                    return
                try:
                    self.task_result_dict[id] = future.result()
                except Exception as err:
                    self.logger.error(err)

        self.set_process_env()
        self.initialize_dirac_api()
        self.executor = ThreadPoolExecutor()
        while not self.stop_event.is_set():
            try:
                task_id, cmd, args_dict = self.task_queue.get_nowait()
                future = self.executor.submit(self.run_dirac_command, cmd, args_dict)
                future.add_done_callback(functools.partial(send_result, task_id))
            except Empty:
                time.sleep(0.05)
                pass
            except RuntimeError:
                self.logger.warn('DIRAC subprocess: Tried to submit after the interpreter shutdown. Returning...')
                self.handle_termination()
        self.handle_termination()

    def run_dirac_command(self, cmd, args_dict):
        from DIRAC.Interfaces.API.Dirac import Dirac  # type: ignore
        dirac = Dirac()
        args_dict['dirac'] = dirac
        return_value = cmd(**args_dict)
        return return_value


class LoggerWriter:
    """
    This is a simple class implementing the basic methods to replace the subprocess stdout and stderr streams
    and pipe their outputs to Ganga's main logging module.
    """
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message != '\n':
            self.logger.log(self.level, message)

    def flush(self):
        pass

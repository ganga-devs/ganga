import functools
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Process


class DiracProcess(Process):
    def __init__(self, task_queue, task_result_dict, env=None):
        super(Process, self).__init__()
        self.daemon = True
        self.task_queue = task_queue
        self.task_result_dict = task_result_dict

    def initialize_dirac_api(self):
        from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script  # type: ignore
        Script.parseCommandLine(ignoreErrors=False)

    def run(self):
        def send_result(event, id, lock, future):
            with lock:
                self.task_result_dict[id] = future.result()
                event.set()

        self.initialize_dirac_api()
        executor = ThreadPoolExecutor()
        lock = Lock()
        while True:
            is_done, task_id, cmd, arg_tuple = self.task_queue.get()
            args, kwargs = arg_tuple
            future = executor.submit(self.run_dirac_command, cmd, *args, **kwargs)
            future.add_done_callback(functools.partial(send_result, is_done, task_id, lock))

    def run_dirac_command(self, cmd, *args, **kwargs):
        from DIRAC.Interfaces.API.Dirac import Dirac  # type: ignore
        dirac = Dirac()
        kwargs['dirac'] = dirac
        return_value = cmd(*args, **kwargs)
        return return_value

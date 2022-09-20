import time
import uuid

from aioprocessing import AioManager, AioQueue
from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError

from .DiracExecutorProcess import DiracProcess


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AsyncDiracManager(metaclass=Singleton):
    def __init__(self, env=None):
        self.dirac_process = None
        self.manager = None
        self.task_queue = AioQueue()
        self.task_result_dict = {}
        self.initialized = False
        self.env = env

    def start_dirac_process(self):
        self.manager = AioManager()
        self.task_result_dict = self.manager.dict()
        self.dirac_process = DiracProcess(task_queue=self.task_queue, task_result_dict=self.task_result_dict)
        self.dirac_process.start()
        self.initialized = True

    def parse_command_result(self, result, return_raw_dict=False):
        if isinstance(result, dict):
            if return_raw_dict:
                # If the output is a dictionary return and it has been requested, then return it
                return result
            # If the output is a dictionary allow for automatic error detection
            if result['OK']:
                return result['Value']
            else:
                raise GangaDiracError(result['Message'])
        else:
            # Else raise an exception as it should be a dictionary
            raise GangaDiracError(result)

    async def execute(self, cmd, return_raw_dict=False, *args, **kwargs):
        if not self.initialized:
            self.start_dirac_process()

        task_id = uuid.uuid4()
        task_done = self.manager.AioEvent()
        await self.task_queue.coro_put((task_done, task_id, cmd, (args, kwargs)))

        t1 = time.perf_counter()
        await task_done.coro_wait()

        t2 = time.perf_counter()
        print(f'{task_id}: Executed task in {t2-t1:.4f} seconds. Task queue: {self.task_queue.qsize()}')

        dirac_result = self.task_result_dict.get(task_id)
        del self.task_result_dict[task_id]

        returnable = self.parse_command_result(dirac_result, return_raw_dict)
        return returnable

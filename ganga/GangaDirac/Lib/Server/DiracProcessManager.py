import os
import uuid
import psutil

from aioprocessing import AioManager, AioQueue
from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError, getDiracEnv
from GangaCore.GPIDev.Credentials import credential_store
from GangaCore.Utility.logging import getLogger

from .DiracExecutorProcess import DiracProcess


logger = getLogger()


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AsyncDiracManager(metaclass=Singleton):
    def __init__(self):
        self.dirac_process = None
        self.manager = None
        self.task_queues = {}
        self.task_result_dicts = {}
        self.active_processes = {}
        self.task_queue = AioQueue()

    def prepare_process_env(self, env=None, cred_req=None):
        if env is None:
            if cred_req is None:
                env = getDiracEnv()
            else:
                env = getDiracEnv(cred_req.dirac_env)
        if cred_req is not None:
            env['X509_USER_PROXY'] = credential_store[cred_req].location
            if os.getenv('KRB5CCNAME'):
                env['KRB5CCNAME'] = os.getenv('KRB5CCNAME')

        return env

    def hash_dirac_env(self, dirac_env):
        # This function hashes an env dict to be used as a dictionary key for our active processes
        return hash(frozenset(dirac_env.items()))

    def start_dirac_process(self, dirac_env=None):
        self.manager = AioManager()
        self.task_result_dict = self.manager.dict()
        env_hash = self.hash_dirac_env(dirac_env)
        self.task_queues[env_hash] = AioQueue()
        self.task_result_dicts[env_hash] = self.manager.dict()
        dirac_process = DiracProcess(
            task_queue=self.task_queues[env_hash],
            task_result_dict=self.task_result_dicts[env_hash],
            env=dirac_env)
        dirac_process.start()
        print(f"DIRAC process started with PID {dirac_process.pid}")
        self.active_processes[env_hash] = dirac_process.pid

    def parse_command_result(self, result, cmd, return_raw_dict=False):
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

    def is_dirac_process_active(self, env_hash):
        if env_hash not in self.active_processes:
            return False
        pid = self.active_processes[env_hash]
        if not psutil.pid_exists(pid):
            return False
        return True

    async def execute(self, cmd, args_dict=None, return_raw_dict=False, env=None, cred_req=None):
        dirac_env = self.prepare_process_env(env, cred_req)
        env_hash = self.hash_dirac_env(dirac_env)

        if not self.is_dirac_process_active(env_hash):
            self.start_dirac_process(dirac_env)

        task_id = uuid.uuid4()
        task_done = self.manager.AioEvent()
        await self.task_queues[env_hash].coro_put((task_done, task_id, cmd, args_dict))

        await task_done.coro_wait()
        dirac_result = self.task_result_dicts[env_hash].get(task_id)
        del self.task_result_dicts[env_hash][task_id]

        returnable = self.parse_command_result(dirac_result, str(cmd), return_raw_dict)
        print(f'Executed DIRAC command {cmd} with result {returnable}')
        return returnable

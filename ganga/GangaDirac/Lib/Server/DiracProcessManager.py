import asyncio
import os
import traceback
import uuid
import psutil

from UltraDict import UltraDict
from UltraDict.Exceptions import AlreadyClosed
from multiprocessing import Queue, Event
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
        self.task_queues = {}
        self.task_result_dicts = {}
        self.stop_events = {}
        self.active_processes = {}
        self.processes_killed = False

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
        env_hash = self.hash_dirac_env(dirac_env)
        self.task_queues[env_hash] = Queue()
        self.stop_events[env_hash] = Event()
        try:
            self.task_result_dicts[env_hash] = UltraDict(recurse=False, auto_unlink=True)
            dirac_process = DiracProcess(
                task_queue=self.task_queues[env_hash],
                task_result_dict_name=self.task_result_dicts[env_hash].name,
                stop_event=self.stop_events[env_hash],
                logger=logger,
                env=dirac_env)
            dirac_process.start()
            logger.debug(f"DIRAC process started with PID {dirac_process.pid}")
            self.active_processes[env_hash] = dirac_process
        except Exception:
            logger.exception(traceback.format_exc())

    def parse_command_result(self, result, return_raw_dict=False, env_hash=None):
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
        process = self.active_processes[env_hash]
        if not psutil.pid_exists(process.pid):
            return False
        return True

    async def execute(self, cmd, args_dict=None, return_raw_dict=False, env=None, cred_req=None):
        dirac_env = self.prepare_process_env(env, cred_req)
        env_hash = self.hash_dirac_env(dirac_env)

        if not self.is_dirac_process_active(env_hash):
            self.start_dirac_process(dirac_env)

        try:
            task_id = uuid.uuid4()
            logger.debug(f'Executing command {str(cmd)}...')
            self.task_queues[env_hash].put((task_id, cmd, args_dict))

            task_results_dict = self.task_result_dicts[env_hash]
            dirac_result = await self.get_task_result(task_id, task_results_dict)

            returnable = self.parse_command_result(dirac_result, return_raw_dict, env_hash)
            logger.debug(f'Executed DIRAC command {cmd} with args {str(args_dict)} and result {returnable}')
            return returnable
        except AlreadyClosed:
            msg = """Tried to access shared DIRAC executor memory after interpreter shutdown.
            This may happen when exiting Ganga while a DIRAC job is completing"""
            logger.warn(msg)
            self.kill_dirac_processes()
        except Exception as err:
            raise GangaDiracError(err)

    async def get_task_result(self, task_id, result_dict):
        while not result_dict.closed and task_id not in result_dict:
            await asyncio.sleep(0.3)
        if result_dict.closed:
            raise AlreadyClosed('Tried to access shared dict after it was already closed!')
        else:
            result = result_dict.get(task_id)
        return result

    def kill_dirac_processes(self):
        if self.processes_killed:
            return
        self.processes_killed = True
        if not self.active_processes:
            return
        for env_hash, process in self.active_processes.items():
            self.stop_events[env_hash].set()
            process.join()
            logger.debug(f"Terminated DIRAC executor process with pid {process.pid}")
        self.active_processes = {}

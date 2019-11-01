import os
import base64
import subprocess
import threading
import pickle as pickle
import signal
from copy import deepcopy
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.logging import getLogger
logger = getLogger()


def bytes2string(obj):
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    if isinstance(obj, dict):
        return {bytes2string(key): bytes2string(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [bytes2string(item) for item in obj]
    if isinstance(obj, tuple):
        return tuple(bytes2string(item) for item in obj)
    return obj


def env_update_script(indent=''):
    """ This function creates an extension to a python script, or just a python script to be run at the end of the
    piece of code we're interested in.
    This piece of code will dump the environment after the execution has taken place into a temporary file.
    This returns a tuple of the script it's generated and the pipes file handlers used to store the end in memory
    Args:
        indent (str): This is the indent to apply to the script if this script is to be appended to a python file
    """
    fdread, fdwrite = os.pipe()
    os.set_inheritable(fdwrite, True)
    this_script = '''
import os
import pickle as pickle
with os.fdopen(###FD_WRITE###,'wb') as envpipe:
    pickle.dump(dict(os.environ), envpipe, 2)
'''
    from GangaCore.GPIDev.Lib.File.FileUtils import indentScript
    script = indentScript(this_script, '###INDENT###')

    script = script.replace('###INDENT###'  , indent      )\
                   .replace('###FD_READ###' , str(fdread) )\
                   .replace('###FD_WRITE###', str(fdwrite))

    return script, (fdread, fdwrite)


def python_wrapper(command, python_setup='', update_env=False, indent=''):
    """ This section of code wraps the given python command inside a small wrapper class to allow us to control the output.
    Optionally we can also append to the end of this file a script to allow us to extract the environment after we've
    finished executing our command.
    Args:
        command (str): This is the python code to be executed (can be multi-line)
        python_setup (str): This is some python code to be executed before the python code in question (aka a script header.
        update_env (bool): Contol whether we want to capture the env after running
        indent (str): This allows for an indent to be applied to the script so it can be placed inside other python scripts
    This returns the file handler objects for the env_update_script, the python wrapper itself and the script which has been generated to be run
    """
    fdread, fdwrite = os.pipe()
    os.set_inheritable(fdwrite, True)
    this_script = '''
from __future__ import print_function
import os, sys, traceback
import pickle as pickle
with os.fdopen(###PKL_FDWRITE###, 'wb') as PICKLE_STREAM:
    def output(data):
        pickle.dump(data, PICKLE_STREAM, 2)
    local_ns = {'pickle'        : pickle,
                'PICKLE_STREAM' : PICKLE_STREAM,
                'output'        : output}
    try:
        full_command = """###SETUP### """
        full_command += """ \n###COMMAND### """
        exec(full_command, local_ns)
    except:
        pickle.dump(traceback.format_exc(), PICKLE_STREAM, 2)

'''
    from GangaCore.GPIDev.Lib.File.FileUtils import indentScript
    script = indentScript(this_script, '###INDENT###')

    script = script.replace('###INDENT###'     , indent              )\
                   .replace('###SETUP###'      , python_setup.strip())\
                   .replace('###COMMAND###'    , command.strip()     )\
                   .replace('###PKL_FDREAD###' , str(fdread)         )\
                   .replace('###PKL_FDWRITE###', str(fdwrite)        )
    env_file_pipes = None
    if update_env:
        update_script, env_file_pipes = env_update_script()
        script += update_script
    return script, (fdread, fdwrite), env_file_pipes


def __reader(pipes, output_ns, output_var, require_output):
    """ This function un-pickles a pickle from a file and return it as an element in a dictionary
    Args:
        pipes (tuple): This is a tuple containing the (read_pipe, write_pipe) from os.pipes containing the pickled object
        output_ns (dict): This is the dictionary we should put the un-pickled object
        output_var (str): This is the key we should use to determine where to put the object in the output_ns
        require_output (bool): Should the reader give a warning if the pickle stream is not readable
    """
    os.close(pipes[1])
    with os.fdopen(pipes[0], 'rb') as read_file:
        try:
            # rcurrie this deepcopy hides a strange bug that the wrong dict is sometimes returned from here. Remove at your own risk
            output_ns[output_var] = deepcopy(pickle.load(read_file))
        except UnicodeDecodeError:
            output_ns[output_var] = deepcopy(bytes2string(pickle.load(read_file, encoding="bytes")))
        except Exception as err:
            if require_output:
                logger.error('Error getting output stream from command: %s', err)


def __timeout_func(process, timed_out):
    """ This function is used to kill functions which are timing out behind the scenes and taking longer than a
    threshold time to execute.
    Args:
        process (class): This is a subprocess class which knows of the pid of wrapping thread around the command we want to kill
        timed_out (Event): A threading event to be set when the command has timed out
    """
        
    if process.returncode is None:
        timed_out.set()
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except Exception as e:
            logger.error("Exception trying to kill process: %s" % e)


def start_timer(p, timeout):
    """ Function to construct and return the timer thread and timed_out
    Args:
        p (object): This is the subprocess object which will be used to run the command of interest
        timeout (int): This is the timeout in seconds after which the command will be killed
    """
    # Start the background thread to catch timeout events
    timed_out = threading.Event()
    timer = threading.Timer(timeout, __timeout_func, args=(p, timed_out))
    timer.daemon = True
    if timeout is not None:
        timer.start()
    return timer, timed_out


def update_thread(pipes, thread_output, output_key, require_output):
    """ Function to construct and return background thread used to read a pickled object into the thread_output for updating
        the environment after executing a users code
        Args:
            started_threads (list): List containing background threads which have been started
            pipes (tuple): Tuple containing (read_pipe, write_pipe) which is the pipe the pickled obj is written to
            thread_output (dict): Dictionary containing the thread outputs which are used after executing the command
            output_key (str): Used to know where in the thread_output to store the output of this thread
            require_output (bool): Does the reader require valid pickled output.
    """
    ev = threading.Thread(target=__reader, args=(pipes, thread_output, output_key, require_output))
    ev.daemon = True
    ev.start()
    return ev


def execute(command,
            timeout=None,
            env=None,
            cwd=None,
            shell=True,
            python_setup='',
            eval_includes=None,
            update_env=False,
            ):
    """
    Execute an external command.
    This will execute an external python command when shell=False or an external bash command when shell=True
    Args:
        command (str): This is the command that we want to execute in string format
        timeout (int): This is the timeout which we want to assign to a function and it will be killed if it runs for longer than n seconds
        env (dict): This is the environment to use for launching the new command
        cwd (str): This is the cwd the command is to be executed within.
        shell (bool): True for a bash command to be executed, False for a command to be executed within Python
        python_setup (str): A python command to be executed beore the main command is
        eval_includes (str): An string used to construct an environment which, if passed, is used to eval the stdout into a python object
        update_env (bool): Should we update the env being passed to what the env was after the command finished running
    """

    if update_env and env is None:
        raise GangaException('Cannot update the environment if None given.')

    if not shell:
        # We want to run a python command inside a small Python wrapper
        stream_command = 'python -'
        command, pkl_file_pipes, env_file_pipes = python_wrapper(command, python_setup, update_env)
    else:
        # We want to run a shell command inside a _NEW_ shell environment.
        # i.e. What I run here I expect to behave in the same way from the command line after I exit Ganga
        stream_command = "bash "
        if update_env:
            # note the exec gets around the problem of indent and base64 gets
            # around the \n
            command_update, env_file_pipes = env_update_script()
            command += ''';python -c "import base64;exec(base64.b64decode(%s))"''' % base64.b64encode(command_update.encode("utf-8"))

    # Some minor changes to cleanup the getting of the env
    if env is None:
        env = os.environ

    # Construct the object which will contain the environment we want to run the command in
    p = subprocess.Popen(stream_command, shell=True, env=env, cwd=cwd, preexec_fn=os.setsid,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True, close_fds=False)

    # This is where we store the output
    thread_output = {}

    # Start the timer thread used to kill commands which have likely stalled
    timer, timed_out = start_timer(p, timeout)

    if update_env:
        env_output_key = 'env_output'
        update_env_thread = update_thread(env_file_pipes, thread_output, env_output_key, require_output=True)
    if not shell:
        pkl_output_key = 'pkl_output'
        update_pkl_thread = update_thread(pkl_file_pipes, thread_output, pkl_output_key, require_output=False)

    # Execute the main command of interest
    logger.debug("Executing Command:\n'%s'" % str(command))
    stdout, stderr = p.communicate(command)

    # Close the timeout watching thread
    logger.debug("stdout: %s" % stdout)
    logger.debug("stderr: %s" % stderr)

    timer.cancel()
    if timeout is not None:
        timer.join()

    # Finish up and decide what to return
    if stderr != '':
        # this is still debug as using the environment from dirac default_env maked a stderr message dump out
        # even though it works
        logger.debug(stderr)

    if timed_out.isSet():
        return 'Command timed out!'

    # Decode any pickled objects from disk
    if update_env:
        update_env_thread.join()
        if env_output_key in thread_output:
            env.update(thread_output[env_output_key])
        else:
            logger.error("Expected to find the updated env after running a command")
            logger.error("Command: %s" % command)
            logger.error("stdout: %s" % stdout)
            logger.error("stderr: %s" % stderr)
            raise RuntimeError("Missing update env after running command")

    if not shell and not eval_includes:
        update_pkl_thread.join()
        if pkl_output_key in thread_output:
            return thread_output[pkl_output_key]

    stdout_temp = None
    try:
        # If output
        if stdout:
            if isinstance(stdout, bytes):
                stdout_temp = pickle.loads(stdout)
            else:
                try:
                    stdout_temp = pickle.loads(stdout.encode("utf-8"))
                except pickle.UnpicklingError:
                    stdout_temp = pickle.loads(stdout.encode("latin1"))
    # Downsides to wanting to be explicit in how this failed is you need to know all the ways it can!
    except (pickle.UnpicklingError, EOFError, ValueError) as err:
        if not shell:
            log = logger.error
        else:
            log = logger.debug
        log("Command Failed to Execute:\n%s" % command)
        log("Command Output is:\n%s" % stdout)
        log("Error received:\n%s" % err)

    if not stdout_temp:
        local_ns = locals()
        if isinstance(eval_includes, str):
            try:
                exec(eval_includes, {}, local_ns)
            except:
                logger.debug("Failed to eval the env, can't eval stdout")
                pass
        if isinstance(stdout, str) and stdout:
            try:
                stdout_temp = eval(stdout, {}, local_ns)
            except Exception as err2:
                logger.debug("Err2: %s" % str(err2))
                pass

    if stdout_temp:
        stdout = stdout_temp

    return stdout


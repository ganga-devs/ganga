import os
import base64
import subprocess
import threading
import pickle
import signal
from tempfile import NamedTemporaryFile
from Ganga.Core.exceptions import GangaException
from Ganga.Utility.logging import getLogger
logger = getLogger()

#execute_lock = threading.RLock()

def env_update_script(indent=''):
    """ This function creates an extension to a python script, or just a python script to be run at the end of the
    piece of code we're interedted in.
    This piece of code will dump the environment after the execution has taken place into a temporary file.
    This returns a tuple of the script it's generated and the NamedTemporaryFile that will be used to store the env
    Args:
        indent (str): This is the indent to apply to the script if this script is to be appended to a python file
    """
    this_env_file = NamedTemporaryFile(delete=False)
    with open(this_env_file.name, 'w') as some_file:
        pass
    this_script = '''
import os, pickle
with open('###TEMP_WRITE###','w') as envpipe:
    pickle.dump(os.environ, envpipe)
'''
    from Ganga.GPIDev.Lib.File.FileUtils import indentScript
    script = indentScript(this_script, '###INDENT###')

    script =  script.replace('###INDENT###'    , indent            )\
                    .replace('###TEMP_WRITE###', this_env_file.name)

    return script, this_env_file

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def python_wrapper(command, python_setup='', update_env=False, indent=''):
    """ This section of code wraps the given python command inside a small wrapper class to allow us to control the output.
    Optionally we can also append to the end of this file a script to allow us to extract the environment after we've
    finished executing our command.
    Args:
        command (str): This is the pthon code to be executed (can be multi-line)
        python_setup (str): This is some python code to be executed before the python code in question (aka a script header.
        update_env (bool): Contol whether we want to capture the env after running
        indent (str): This allows for an indent to be applied to the script so it can be placed inside other python scripts
    This returns the NamedTemporaryFile objects for the env_update_script, the python wrapper itself and the script which has been generated to be run
    """
    this_pkl_file = NamedTemporaryFile(delete=False)
    with open(this_pkl_file.name, 'w') as some_file:
        pass
    this_script = '''
from __future__ import print_function
import os, sys, pickle, traceback
with open('###TEMP_WRITE###', 'w') as PICKLE_STREAM:
    def output(data):
        print(pickle.dumps(data), file=PICKLE_STREAM)
    local_ns = {'pickle'        : pickle,
                'PICKLE_STREAM' : PICKLE_STREAM,
                'output'        : output}
    try:
        full_command = """###SETUP### """
        full_command += """ \n###COMMAND### """
        exec(full_command, local_ns)
    except:
        print(pickle.dumps(traceback.format_exc()), file=PICKLE_STREAM)
'''
    from Ganga.GPIDev.Lib.File.FileUtils import indentScript
    script = indentScript(this_script, '###INDENT###')

    script =  script.replace('###INDENT###'     , indent              )\
                    .replace('###SETUP###'      , python_setup.strip())\
                    .replace('###COMMAND###'    , command.strip()     )\
                    .replace('###TEMP_WRITE###' , this_pkl_file.name  )
    this_env_file = None
    if update_env:
        update_script, this_env_file = env_update_script()
        script += update_script
    return script, this_pkl_file, this_env_file

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

def __reader(temp_file, output_ns, output_var):
    """ This function un-pickles a pickle from a file and return it as an element in a dictionary
    Args:
        temp_file (str): This is the name of the file containing the pickle
        output_ns (dict): This is the dictionary we should put the un-pickled object
        outptu_var (str): This is the key we should use to determine where to put the object in the output_ns
    """
    logger.debug("Accessing: %s" % temp_file)
    with open(temp_file, 'r') as read_file:
        data = read_file.read()

    try:
        data = pickle.loads(data)
        output_ns.update({output_var: data})
    except Exception as err:
        logger.error("Err: %s" % str(err))
        raise  # EOFError triggered if command killed with timeout

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


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

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


#def execute(command, timeout=None,env=None,cwd=None,shell=True,python_setup='',eval_includes=None,update_env=False,return_code=None):
#    with execute_lock:
#        return _execute(command, timeout, env, cwd, shell, python_setup, eval_includes, update_env, return_code)

def execute(command,
            timeout=None,
            env=None,
            cwd=None,
            shell=True,
            python_setup='',
            eval_includes=None,
            update_env=False,
            return_code=None):
    """
    Execute an external command.
    This will execute an external python command when shell=False or an external bash command when shell=True
    Args:
        command (str): This is the command that we want to execute in string format
        timeout (int): This is the timeout which we want to assign to a funtion and it will be killed if it runs for longer than n seconds
        env (dict): This is the environment to use for launching the new command
        cwd (str): This is the cwd the command is to be executed within.
        shell (bool): True for a bash command to be executed, False for a command to be executed within Python
        python_setup (str): A python command to be executed beore the main command is
        eval_includes (str): A string to be executed in the namespace of the output of the command
        update_env (bool): Should we update the env being passed to what the env was after the command finished running
        return_code (int): This is the returned code from the command which is executed
    """

    if update_env and env is None:
        raise GangaException('Cannot update the environment if None given.')

    temp_pkl_file = None
    temp_env_file = None

    if not shell:
        # We want to run a pyhton command inside a small Python wrapper
        stream_command = 'python -'
        command, temp_pkl_file, temp_env_file = python_wrapper(command, python_setup, update_env)
    else:
        # We want to run a shell command inside a _NEW_ shell environment.
        # i.e. What I run here I expect to behave in the same way from the command line after I exit Ganga
        stream_command = "bash -i "
        if update_env:
            # note the exec gets around the problem of indent and base64 gets
            # around the \n
            command_update, temp_env_file = env_update_script()
            command += ''';python -c "import base64;exec(base64.b64decode('%s'))"''' % base64.b64encode(command_update)

    # TODO make this into a small support function not in execute
    if env is None and not update_env:
        # If we're not updating the environment, and the environment ie empty we need to create a new environment to be use by the command
        pipe = subprocess.Popen('python -c "from __future__ import print_function;import os;print(os.environ)"',
                                env=None, cwd=None, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        output = pipe.communicate()
        env = eval(output[0])

        if env:
            for k, v in env.iteritems():
                if not str(v).startswith('() {'):
                    env[k] = os.path.expandvars(v)
                # Be careful with exported bash functions!
                else:
                    this_string = str(v).split('\n')
                    final_str = ""
                    for line in this_string:
                        final_str += str(os.path.expandvars(line)).strip()
                    if not final_str.endswith(';'):
                        final_str += " ;"
                    final_str += " "

    # Construct the class which will contain the environment we want to run the command in
    p = subprocess.Popen(stream_command,
                         shell=True,
                         env=env,
                         cwd=cwd,
                         preexec_fn=os.setsid,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    # Start the background thread to catch timeout events
    timed_out = threading.Event()
    timer = threading.Timer(timeout, __timeout_func, args=(p, timed_out))
    timer.daemon = True
    started_threads = []
    if timeout is not None:
        timer.start()
        started_threads.append(timer)

    # This is where we store the output
    thread_output = {}

    # Execute the main command of interest
    logger.debug("Executing Command:\n'%s'" % str(command))
    stdout, stderr = p.communicate(command)

    # Close the timeout watching thread
    logger.debug("stdout: %s" % stdout)
    logger.debug("stderr: %s" % stderr)
    timer.cancel()
    for t in started_threads:
        t.join()


    # Decode any pickled objects from disk
    if update_env:
        try:
            __reader(temp_env_file.name, thread_output, 'env_output')
        except Exception as err:
            logger.error("Failed to Update Env after command:")# %s" % command)
            logger.error("Error was: %s" % err)
            logger.error("The pickle we tried to read was in: %s" % temp_env_file.name)
            logger.error("stdout was: %s" % stdout)
            logger.error("stderr was: %s" % stderr)
            raise
                                                        
    if not shell:
        try:
            __reader(temp_pkl_file.name, thread_output, 'pkl_output')
        except Exception as err:
            logger.error("Failed to Pickle output from command: %s" % command)
            logger.error("Error was: %s" % err)
            logger.error("The pickle we tried to read was in: %s" % temp_pkl_file.name)
            logger.error("stdout was: %s" % stdout)
            logger.error("stderr was: %s" % stderr)
            raise

    # Cleanup after ourselves
    for file_ in [temp_env_file, temp_pkl_file]:
        if file_:
            os.unlink(file_.name)


    # Finish up and decide what to return
    if stderr != '':
        # this is still debug as using the environment from dirac default_env maked a stderr message dump out
        # even though it works
        logger.debug(stderr)

    if timed_out.isSet():
        return 'Command timed out!'

    if update_env and 'env_output' in thread_output:
        env.update(thread_output['env_output'])

    if not shell and 'pkl_output' in thread_output:
        return thread_output['pkl_output']

    try:
        stdout = pickle.loads(stdout)
    except Exception as err:
        logger.debug("Err: %s" % str(err))
        local_ns = {}
        if isinstance(eval_includes, str):
            exec(eval_includes, {}, local_ns)
        try:
            stdout = eval(stdout, {}, local_ns)
        except Exception as err2:
            logger.debug("Err2: %s" % str(err2))
            pass

    return_code = p.returncode
    return stdout


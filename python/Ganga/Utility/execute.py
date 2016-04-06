import os
import base64
import subprocess
import threading
import pickle
import signal
from Ganga.Core.exceptions import GangaException
from Ganga.Utility.logging import getLogger
logger = getLogger()

def env_update_script(indent=''):
    fdread, fdwrite = os.pipe()
    this_script = '''
import os, pickle
os.close(###FD_READ###)
with os.fdopen(###FD_WRITE###,'wb') as envpipe:
    pickle.dump(os.environ, envpipe)
'''
    from Ganga.GPIDev.Lib.File.FileUtils import indentScript
    script = indentScript(this_script, '###INDENT###')

    script =  script.replace('###INDENT###'  , indent      )\
                    .replace('###FD_READ###' , str(fdread) )\
                    .replace('###FD_WRITE###', str(fdwrite))
    return script, fdread, fdwrite

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def python_wrapper(command, python_setup='', update_env=False, indent=''):
    fdread, fdwrite = os.pipe()
    this_script = '''
from __future__ import print_function
import os, sys, pickle, traceback
os.close(###PKL_FDREAD###)
with os.fdopen(###PKL_FDWRITE###, 'wb') as PICKLE_STREAM:
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
                    .replace('###PKL_FDREAD###' , str(fdread)         )\
                    .replace('###PKL_FDWRITE###', str(fdwrite))
    envread = None,
    envwrite = None
    if update_env:
        update_script, envread, envwrite = env_update_script()
        script += update_script
    return script, fdread, fdwrite, envread, envwrite

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def __reader(readfd, writefd, output_ns, output_var):
    os.close(writefd)
    with os.fdopen(readfd, 'rb') as read_file:
        try:
            output_ns.update({output_var: pickle.load(read_file)})
        except Exception as err:
            logger.debug("Err: %s" % str(err))
            pass  # EOFError triggered if command killed with timeout

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def __timeout_func(process, timed_out):
    if process.returncode is None:
        timed_out.set()
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except Exception as e:
            logger.error("Exception trying to kill process: %s" % e)

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


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
    """

    if update_env and env is None:
        raise GangaException('Cannot update the environment if None given.')

    stream_command = 'cat<&0 | sh'
    if not shell:
        stream_command = 'python -'
        command, pkl_read, pkl_write, envread, envwrite = python_wrapper(command, python_setup, update_env)
    elif update_env:
        # note the exec gets around the problem of indent and base64 gets
        # around the \n
        command_update, envread, envwrite = env_update_script()
        command += ''';python -c ""from __future__ import print_function;import base64;exec(base64.b64decode('%s'))"''' % base64.b64encode(command_update)

    if env is None and not update_env:
        pipe = subprocess.Popen('python -c "from __future__ import print_function;import os;print(os.environ)"',
                                env=None, cwd=None, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        output = pipe.communicate()
        env = eval(eval(str(output))[0])

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

    p = subprocess.Popen(stream_command,
                         shell=True,
                         env=env,
                         cwd=cwd,
                         preexec_fn=os.setsid,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    timed_out = threading.Event()
    timer = threading.Timer(timeout, __timeout_func,
                            args=(p, timed_out))
    timer.deamon = True
    started_threads = []
    if timeout is not None:
        timer.start()
        started_threads.append(timer)

    thread_output = {}
    if not shell:
        ti = threading.Thread(target=__reader,
                              args=(pkl_read, pkl_write,
                                    thread_output, 'pkl_output'))
        ti.deamon = True
        ti.start()
        started_threads.append(ti)

    if update_env:
        ev = threading.Thread(target=__reader,
                              args=(envread, envwrite,
                                    thread_output, 'env_output'))
        ev.deamon = True
        ev.start()
        started_threads.append(ev)

    #logger.debug("Executing Command:\n'%s'" % str(command))

    stdout, stderr = p.communicate(command)
    timer.cancel()

    for t in started_threads:
        t.join()

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


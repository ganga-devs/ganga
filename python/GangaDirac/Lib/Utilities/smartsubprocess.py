import subprocess
from subprocess import STDOUT, PIPE
import os, collections

CommandOutput = collections.namedtuple('CommandOutput', ['returncode', 'stdout', 'stderr'])

class Popen(subprocess.Popen):

    def _proc_timeout(self, timeout, errwrite, envread, env, bufsize, callback_func, cbfunc_args, cbfunc_kwds):
        import time, os
        time_start = time.time()
        while self.poll() is None:
            if timeout is not None:
                if time.time()-time_start >= timeout:
                    with os.fdopen(errwrite, 'ab', bufsize) as f:
                        f.write("Command timed out!")
                    self.returncode = -9
                    self.kill()
                    #try: # can throw exception if they call communicate without pipe
                        #self.wait()
                    #except: pass
                    break
            time.sleep(0.5)
        else:
            if timeout is not None:
                os.close(errwrite)
        
        if envread is not None:
            import pickle
            if type(env) != dict:
                raise Exception("Env dict not passed properly.")
            with os.fdopen(envread,'r', bufsize) as f:
                env.update(pickle.loads(f.read()))

        if callback_func is not None:
            stdout, stderr = self.communicate()
            callback_func(CommandOutput(self.returncode, stdout, stderr), *cbfunc_args, **cbfunc_kwds)

        # This can be used to inspect the open file descriptors
        # print os.system('ls -l /proc/$$/fd')
        # Might want to pass all the remaining fd to close at end. cant close pipes though as communiate will read them

    def __init__( self,
                  args,
                  bufsize            = 0,
                  executable         = None,
                  stdin              = None,
                  stdout             = None,
                  stderr             = None,
                  preexec_fn         = None,
                  close_fds          = False,
                  shell              = False,
                  cwd                = None,
                  env                = None,
                  universal_newlines = False,
                  startupinfo        = None,
                  creationflags      = 0,
                  timeout            = None,
                  update             = False,
                  callback_func      = None,
                  cbfunc_args        = (),
                  cbfunc_kwds        = {},
                  usepython          = False):

        if usepython:
            if type(args) != str:
                raise Exception('To run a python command/script give the arg as type str')
            if not shell:
                raise Exception('To run a python command/script use the shell=True option')
            if close_fds:
                raise Exception('To run a python command/script use the close_fds=False option')
            script = args
            inread, inwrite = os.pipe()
            # allows arbitrary length python scripts (with indents) to be read in on command line
            args = '''python -c "import os; os.close(%i); exec(os.fdopen(%i, 'rb', %i).read())"''' % (inwrite, inread, bufsize)
            
        outwrite = stdout
        errwrite = stderr
        if timeout is not None:
            if close_fds:
                raise Exception('To run with a timeout use the close_fds=False option')
            # essentially handling the PIPEing ourselves so super constructor just sees
            # file descriptors instead and never PIPE
            if stderr == PIPE:
                errread, errwrite = os.pipe()
            elif stderr == STDOUT and stdout == PIPE:
                outread, outwrite = os.pipe()

        envread=None
        if update:
            if type(args)!= str:
                raise Exception('To update the environment please use the args as type str')    
            if not shell:
                raise Exception('To update the environment please use the shell=True option')
            if env is None:
                raise Exception('To update the environment you must pass an environment into the env arg as type dict')
            if close_fds:
                raise Exception('To update the environment please use the close_fds=False option')
            envread, envwrite = os.pipe()
            args += '''; python -c "import os, pickle; os.close(%i); os.fdopen(%i, 'wb', %i).write(pickle.dumps(os.environ))"''' % (envread, envwrite, bufsize)
            
        super(Popen, self).__init__( args               = args,
                                     bufsize            = bufsize,
                                     executable         = executable,
                                     stdin              = stdin,
                                     stdout             = outwrite,
                                     stderr             = errwrite,
                                     preexec_fn         = preexec_fn,
                                     close_fds          = close_fds,
                                     shell              = shell,
                                     cwd                = cwd,
                                     env                = env,
                                     universal_newlines = universal_newlines,
                                     startupinfo        = startupinfo,
                                     creationflags      = creationflags )
           

        if usepython:
            os.close(inread)
            with os.fdopen(inwrite, 'wb', bufsize) as f:
                f.write(script)      

        if timeout is not None or callback_func is not None or update:
            if update:
                os.close(envwrite) # must close the write end in the parent process else read will hang
            
            if timeout is not None:
                import sys
                if stderr is None:
                    errwrite = os.dup(sys.stderr.fileno())
                elif stderr == PIPE:
                    if universal_newlines:
                        self.stderr = os.fdopen(errread,'rU', bufsize)
                    else:
                        self.stderr = os.fdopen(errread,'rb', bufsize)
                elif stderr == STDOUT:
                    if stdout == PIPE:
                        errwrite = outwrite
                        if universal_newlines:
                            self.stdout = os.fdopen(outread,'rU', bufsize)
                        else:
                            self.stdout = os.fdopen(outread,'rb', bufsize)
                    elif stdout is None:
                        errwrite = os.dup(sys.stdout.fileno())
                    elif isinstance(stdout, int):
                        errwrite = os.dup(stdout)
                    elif isinstance(stdout, file):
                        errwrite = os.dup(stdout.fileno())
                elif isinstance(stderr, int):
                    errwrite = os.dup(stderr)
                elif isinstance(stderr, file):
                    errwrite = os.dup(stderr.fileno())

            from threading import Thread
            t=Thread(target=self._proc_timeout,
                     kwargs={'timeout'       : timeout,
                             'errwrite'      : errwrite,
                             'envread'       : envread,
                             'env'           : env,
                             'bufsize'       : bufsize,
                             'callback_func' : callback_func,
                             'cbfunc_args'   : cbfunc_args,
                             'cbfunc_kwds'   : cbfunc_kwds
                             })
            t.deamon=True        
            t.start()       
            

            

def runcmd(cmd, timeout=None, env=None, cwd=None, update=False, usepython=False):
    p=Popen(cmd, shell=True, timeout=timeout, env=env, cwd=cwd, update=update, usepython=usepython, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    return CommandOutput(p.returncode, stdout, stderr)


def runcmd_async(cmd, timeout=None, env=None, cwd=None, update=False, usepython=False, callback_func=None, args=(), kwds={}):
    p=Popen(cmd, shell=True, timeout=timeout, env=env, cwd=cwd, update=update, usepython=usepython,
            stdout=PIPE, stderr=PIPE, callback_func=callback_func, cbfunc_args=args, cbfunc_kwds=kwds)        
    return

import subprocess
from subprocess import STDOUT, PIPE
import os, collections

# could use this
CommandOutput = collections.namedtuple('CommandOutput', ['returncode', 'stdout', 'stderr'])


class Popen(subprocess.Popen):

    def _proc_timeout(self, timeout, outwrite, errwrite, envread, env, callback_func, cbfunc_args, cbfunc_kwds):
        import time, os
        time_start = time.time()
        while self.poll() is None:
            if timeout is not None:
                if time.time()-time_start >= timeout:
                    outstream = outwrite
                    errstream = errwrite
                    if outwrite is None:
                        outstream = sys.stdout
                    elif isinstance(outwrite, int):
                        outstream = os.fdopen(outwrite, 'wb')
                    outstream.write('')

                    if errwrite is None:
                        errstream = sys.stderr
                    elif errwrite == STDOUT:
                        errstream = outstream
                    elif isinstance(errwrite, int):
                        errstream = os.fdopen(errwrite, 'wb')
                    errstream.write("Command timed out!")
                    self.kill()
                    self.wait()
                    # cant use the os.close below as the fd will close but leave an open file object which will die when trying to close on destruction
                    if isinstance(outwrite, int):
                        outstream.close()
                    if isinstance(errwrite, int) and errwrite != STDOUT:
                        errstream.close()
                    break
            time.sleep(0.5)
        else:
            if isinstance(outwrite, int):
                os.close(outwrite)
            if isinstance(errwrite, int) and errwrite != STDOUT:
                os.close(errwrite)
        
        if envread is not None:
            if type(env) != dict:
                raise Exception("Env dict not passed properly.")
            envstream = os.fdopen(envread,'r', 0)
            env.update(dict((tuple(line.strip().split('=',1)) for line in envstream.readlines() if len(line.strip().split('=',1)) == 2)))
            envstream.close()

        if callback_func is not None:
            stdout, stderr = self.communicate()
            callback_func(CommandOutput(self.returncode, stdout, stderr), *cbfunc_args, **cbfunc_kwds)

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
            import marshal, base64
            if type(args) != str:
                raise Exception('To run a python command/script give the arg as type str')
            if not shell:
                raise Exception('To run a python command/script use the shell=True option')
            code_string = compile(args, '<string>', 'exec')
            args = '''python -c "import marshal, base64; exec(marshal.loads(base64.b64decode('%s')))"''' % base64.b64encode(marshal.dumps(code_string))

        if timeout is None and callback_func is None and not update: # no need for the thread.
            super(Popen, self).__init__( args               = args,
                                         bufsize            = bufsize,
                                         executable         = executable,
                                         stdin              = stdin,
                                         stdout             = stdout,
                                         stderr             = stderr,
                                         preexec_fn         = preexec_fn,
                                         close_fds          = close_fds,
                                         shell              = shell,
                                         cwd                = cwd,
                                         env                = env,
                                         universal_newlines = universal_newlines,
                                         startupinfo        = startupinfo,
                                         creationflags      = creationflags )
        else:      
            outwrite = stdout
            errwrite = stderr
            envread = None

            # essentially handling the PIPEing ourselves so super constructor just sees
            # file descriptors instead and never PIPE
            if stdout == PIPE:
                outread, outwrite = os.pipe()
                
            if stderr == PIPE:
                errread, errwrite = os.pipe()

            if update:
                if type(args)!= str:
                    raise Exception('To update the environment please use the args as type str')    
                if not shell:
                    raise Exception('To update the environment please use the shell=True option')
                if env is None:
                    raise Exception('To update the environment you must pass an environment into the env arg as type dict')
                envread, envwrite = os.pipe()
                args += '; printenv >&%s' % str(envwrite)# could instead use OS neutral python -c "import os; print os.environ" >&%s

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
        
            if stdout == PIPE:
                if universal_newlines:
                    self.stdout = os.fdopen(outread,'rU', bufsize)
                else:
                    self.stdout = os.fdopen(outread,'rb', bufsize)
            if stderr == PIPE:
                if universal_newlines:
                    self.stderr = os.fdopen(errread,'rU', bufsize)
                else:
                    self.stderr = os.fdopen(errread,'rb', bufsize)
        
            if update:
                os.close(envwrite) # must close the write end in the parent process else read will hang

            from threading import Thread
            t=Thread(target=self._proc_timeout, args=(timeout, outwrite, errwrite, envread, env, callback_func, cbfunc_args, cbfunc_kwds))
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

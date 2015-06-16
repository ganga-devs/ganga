#!/bin/env python
#----------------------------------------------------------------------------
# Name:         Commands.py
# Purpose:      Thread wrapper for the system commands.
#
# Author:       Alexander Soroko
#
# Created:      05/02/2003
#----------------------------------------------------------------------------

import os
import sys
import time
import threading
import popen2
import signal
import PipeReader

MIN_TIMEOUT = 0.01

if sys.platform == 'win32':
    try:
        import win32api
        import win32pipe
        import win32file
        import win32process
        import win32security
        import win32con
        import win32pdh
        import win32pdhutil
        import msvcrt
    except ImportError:
        WIN_EXT = 0
    else:
        WIN_EXT = 1
        MIN_TIMEOUT = 0.1  # on Win32 IsRunning command is slow!

##########################################################################


class winPopen:

    """Class to emulate popen2.Popen3 object on Windows"""

    def __init__(self, cmd, capturestderr=False, bufsize=None):
        try:
            if WIN_EXT:
                # security attributes for pipes
                sAttrs = win32security.SECURITY_ATTRIBUTES()
                sAttrs.bInheritHandle = 1

                # create pipes
                hStdin_r,  self.hStdin_w = win32pipe.CreatePipe(sAttrs, 0)
                self.hStdout_r, hStdout_w = win32pipe.CreatePipe(sAttrs, 0)
                if capturestderr:
                    self.hStderr_r, hStderr_w = win32pipe.CreatePipe(sAttrs, 0)

                # set the info structure for the new process.
                StartupInfo = win32process.STARTUPINFO()
                StartupInfo.hStdInput = hStdin_r
                StartupInfo.hStdOutput = hStdout_w
                if capturestderr:
                    StartupInfo.hStdError = hStderr_w
                else:
                    StartupInfo.hStdError = hStdout_w
                StartupInfo.dwFlags = win32process.STARTF_USESTDHANDLES
                StartupInfo.dwFlags = (StartupInfo.dwFlags |
                                       win32process.STARTF_USESHOWWINDOW)
                StartupInfo.wShowWindow = win32con.SW_HIDE

                # Create new output read handles and the input write handle. Set
                # the inheritance properties to FALSE. Otherwise, the child inherits
                # the these handles; resulting in non-closeable handles to the pipes
                # being created.
                pid = win32api.GetCurrentProcess()
                pp = ['hStdin_w', 'hStdout_r']
                if capturestderr:
                    pp.append('hStderr_r')
                for hh in pp:
                    handle = getattr(self, hh)
                    tmp = win32api.DuplicateHandle(
                        pid,
                        handle,
                        pid,
                        0,
                        0,     # non-inheritable!!
                        win32con.DUPLICATE_SAME_ACCESS)
                    # Close the inhertible version of the handle
                    win32file.CloseHandle(handle)
                    setattr(self, hh, tmp)

                # start the process.
                hProcess, hThread, dwPid, dwTid = win32process.CreateProcess(
                    None,   # program
                    cmd,    # command line
                    None,   # process security attributes
                    None,   # thread attributes
                    1,      # inherit handles, or USESTDHANDLES won't work.
                    # creation flags. Don't access the console.
                    0,      # Don't need anything here.
                    # If you're in a GUI app, you should use
                    # CREATE_NEW_CONSOLE here, or any subprocesses
                    # might fall victim to the problem described in:
                    # KB article: Q156755, cmd.exe requires
                    # an NT console in order to perform redirection..
                    # win32process.CREATE_NEW_CONSOLE,
                    None,   # no new environment
                    None,   # current directory (stay where we are)
                    StartupInfo)
                # normally, we would save the pid etc. here...
                self._hProcess = hProcess
                self._hThread = hThread
                self._dwTid = dwTid
                self.pid = dwPid

                # Child is launched. Close the parents copy of those pipe handles
                # that only the child should have open.
                # You need to make sure that no handles to the write end of the
                # output pipe are maintained in this process or else the pipe will
                # not close when the child process exits and the ReadFile will
                # hang.
                if capturestderr:
                    win32file.CloseHandle(hStderr_w)
                win32file.CloseHandle(hStdout_w)
                win32file.CloseHandle(hStdin_r)

                cfd_fun = msvcrt.open_osfhandle
                md = os.O_TEXT
                self.tochild = os.fdopen(cfd_fun(self.hStdin_w, md), "w")
                self.fromchild = os.fdopen(cfd_fun(self.hStdout_r, md), "r")
                if capturestderr:
                    self.childerr = os.fdopen(cfd_fun(self.hStderr_r, md), "r")

            else:
                raise Exception("Error using Windows extensions")

        except:
            self._hProcess = None
            self._hThread = None
            self._dwTid = None
            self.pid = None
            if capturestderr:
                pfactory = popen2.popen3
            else:
                pfactory = popen2.popen4
            if bufsize:
                pipes = pfactory(cmd, bufsize)
            else:
                pipes = pfactory(cmd)
            if capturestderr:
                (self.fromchild, self.tochild, self.childerr) = pipes
            else:
                (self.fromchild, self.tochild) = pipes

#---------------------------------------------------------------------------
    def poll(self):
        if self.pid:
            if IsRunning(self.pid):
                return -1
            elif WIN_EXT and self._hProcess:
                try:
                    return win32process.GetExitCodeProcess(self._hProcess)
                except:
                    return

#---------------------------------------------------------------------------
    def wait(self):
        while 1:
            status = self.poll()
            if status == -1:
                time.sleep(MIN_TIMEOUT)
            else:
                return status

##########################################################################
# Wrapper for command


class Command:

    """Class to submit a command to the operative system"""

    def __init__(self, cmd, std_in=None, timeout=30,
                 pipesize=0, blocksize=1024, capturestderr=False):
        """
        cmd       = command to be executed.
        std_in    = input for the command.
        timeout   = timeout between blocks of the command output (in seconds).
        pipesize  = the size (in blocks) of the queue used to buffer
                    the blocks read.
        blocksize = the maximum block size for a raw read.
        capturestderr if True tells to merge std_out and std_err of the command.
        """

        # initialization
        self._cmd = cmd
        if std_in:
            self._std_in = std_in
        else:
            self._std_in = []
        self._timeout = timeout
        self._capturestderr = capturestderr

        if sys.platform == 'win32':
            self._pipe_obj = winPopen(self._cmd, capturestderr)
        else:
            if capturestderr:
                self._pipe_obj = popen2.Popen3(self._cmd, 1)
            else:
                self._pipe_obj = popen2.Popen4(self._cmd)

        if capturestderr:
            out_pipes = [self._pipe_obj.fromchild, self._pipe_obj.childerr]
        else:
            out_pipes = [self._pipe_obj.fromchild]
        pipe_in = self._pipe_obj.tochild
        self._pid = self._pipe_obj.pid

        # write std_in
        try:
            try:
                for line in self._std_in:
                    if line and line[-1] != '\n':
                        line += '\n'
                    pipe_in.write(line)
            finally:
                pipe_in.close()
        except:
            pass

        # start reading output
        self._nbpipes = []
        for pipe in out_pipes:
            self._nbpipes.append(PipeReader.PipeReader(pipe, timeout,
                                                       pipesize, blocksize))

#---------------------------------------------------------------------------
    def __del__(self):
        self.finalize()

#---------------------------------------------------------------------------
    def submit(self):
        """Legacy method. Deprecated."""
        pass

#---------------------------------------------------------------------------
    def getInput(self):
        """Return list of input strings."""
        return self._std_in

#---------------------------------------------------------------------------
    def getStatus(self):
        return self._pipe_obj.poll()

#---------------------------------------------------------------------------
    def isRunning(self):
        """Shows is command running or not.
        If there is no way to establish this (win32 without extensions)
        always returns True."""
        if self.getStatus() in [-1, None]:
            return 1
        else:
            return 0

#---------------------------------------------------------------------------
    def _readlines(self, output):
        lines = []
        rest = output
        while rest:
            end = rest.find('\n') + 1
            if end > 0:
                lines.append(rest[:end])
                rest = rest[end:]
            else:
                lines.append(rest)
                rest = ''
        return lines

#---------------------------------------------------------------------------
    def readOutput(self, pipe_ind, maxblocks=0, timeout=None):
        """Read no more than maxblocks from out pipe i.
        i = 0 std_out (or std_out + std_err)
        i = 1 std_err.
        If maxblocks = 0 (default) read till the end of data or timeout
        between blocks arrival"""
        pobj = self._nbpipes[pipe_ind]
        data = pobj.read(maxblocks, timeout, condition=self.isRunning)
        return self._readlines(data)

#---------------------------------------------------------------------------
    def finalize(self):
        """Tries to kill command process,
        close pipes and stop threads.
        Can block on win32 without win extensions."""
        if self.isRunning():
            if self._pid:
                if not Kill(self._pid):
                    print "Warning! Command process is still running"
        for pipe in self._nbpipes:
            pipe.stop()
            try:
                pipe.rfile.close()
            except:
                pass

#---------------------------------------------------------------------------
    def getOutput(self, maxblocks=0):
        """getOutput([maxblocks])
                        --> [std_out and std_err lines],
                            if capturestderr = True
                        --> ([std_out lines], [std_err lines]),
                            if capturestderr = False"""
        output = []
        for i in range(len(self._nbpipes)):
            try:
                outlines = self.readOutput(i, maxblocks)
            except:
                output.append([])
            else:
                output.append(outlines)

        # try to close files and stop threads
        self.finalize()

        if self._capturestderr:
            return tuple(output)

        return output[0]

#---------------------------------------------------------------------------
    def getStatusOutput(self, maxblocks=0):
        """getStatusOutput([maxblocks]) --> (status, getOutput())"""
        output = self.getOutput(maxblocks)
        status = self.getStatus()
        return (status, output)


##########################################################################
# Command to kill a process
def Kill(pid, exitCode=0):
    """Wrapper for os.kill() to kill a process
    Kill(pid [, exitCode]) --> status.
    exitCode is relevant only for win32 platform"""
    try:
        if sys.platform == 'win32':
            if WIN_EXT:
                try:
                    h = win32api.OpenProcess(
                        win32con.PROCESS_TERMINATE, 0, pid)
                    win32api.TerminateProcess(h, exitCode)
                finally:
                    win32api.CloseHandle(h)
            else:
                raise NotImplementedError()
        else:
            os.kill(pid, signal.SIGTERM)
    except:
        return 0
    else:
        return 1

##########################################################################
# returns performance attributes on windows for all processes


def winAllProcesses(object="Process",
                    format=None,
                    machine=None,
                    bRefresh=1):
    """Return a tuple of a list of process attributes and a
    list with the requested attributes for all processes.
    Run only on win32 with windows extensions.
    """
    if not format:
        format = win32pdh.PDH_FMT_LONG

    if bRefresh:  # PDH docs say this is how you do a refresh.
        win32pdh.EnumObjects(None, machine, 0, 1)

    counter = "ID Process"
    items, instances = win32pdh.EnumObjectItems(None, None, object, -1)
    # Track multiple instances.
    instance_dict = {}
    for instance in instances:
        try:
            instance_dict[instance] += 1
        except KeyError:
            instance_dict[instance] = 0

    items = [counter] + items[:5]
    all_pr_attr = []
    get_attr = win32pdhutil.GetPerformanceAttributes
    for instance, max_instances in instance_dict.items():
        for inum in xrange(max_instances + 1):
            try:
                attr_list = []
                for item in items:
                    attr_list.append(get_attr(object, item, instance,
                                              inum, format, machine))
            except:
                continue
            all_pr_attr.append(attr_list)

    return (items, all_pr_attr)

##########################################################################


def ListAllProcesses():
    """List attributes for all running processes."""
    try:
        if sys.platform == 'win32':
            if WIN_EXT:
                items, attr = winAllProcesses()
                items_str = ' \t'.join(map(lambda x: str(x), items)) + '\n'
                return (items_str, attr)
        else:
            uid = os.getuid()
            cmd_line = 'ps -U ' + str(uid) + ' -flw'
            command = Command(cmd_line)
            status, output = command.getStatusOutput()
            if status == 0 and len(output) > 1:
                attr = []
                for line in output[1:]:
                    stat_fields = line.split()
                    attr.append(stat_fields)
                return (output[0], attr)
    except:
        pass
    return ('', [])

##########################################################################


def GetProcessAttributes(pid):
    proc_list = ListAllProcesses()[1]
    for attr in proc_list:
        if sys.platform == 'win32':
            if len(attr) > 0 and attr[0] == pid:
                break
        else:
            if len(attr) > 3 and attr[3] == str(pid):
                break
    else:
        return []
    return attr

##########################################################################
# Command to get process status


def IsRunning(pid):
    """IsRunning(pid) --> status
    pid = pricess ID.
    status = True or False"""
    attr = GetProcessAttributes(pid)
    if sys.platform == 'win32':
        if len(attr) > 0 and attr[0] == pid:
            return 1
    else:
        if len(attr) > 3 and attr[3] == str(pid):
            if attr[1] != 'Z':
                return 1
    return 0

##########################################################################
# helper function


def submitCmd(cmd_line, std_in=[],  timeout=120.0):
    command = Command(cmd_line, std_in, timeout)
    status, output = command.getStatusOutput()
    if status:
        # status not in [0, None]
        executed = 0
    else:
        executed = 1
    return (executed, output)

##########################################################################

import subprocess, datetime, os, time, signal

class Timeout:
    def command(self,command, timeout):
        """call shell-command and either return its output or kill it
           if it doesn't normally exit within timeout seconds and return None"""

        start = datetime.datetime.now()
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while process.poll() is None:
          time.sleep(0.1)
          now = datetime.datetime.now()
          if (now - start).seconds> timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            return 2
          return process.stdout.read()

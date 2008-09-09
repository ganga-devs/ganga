def available_versions(appname):
  """Provide a list of the available Gaudi application versions"""
  
  from Ganga.Utility.Shell import Shell
  s = Shell()
  command = '$LHCBHOME/scripts/SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s" % command)
  versions = output[output.rfind('(')+1:output.rfind('q[uit]')].split()
  return versions

def guess_version(appname):
  """Guess the default Gaudi application version"""
  
  from Ganga.Utility.Shell import Shell
  s = Shell()
  command = '$LHCBHOME/scripts/SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s" % command)
  version = output[output.rfind('[')+1:output.rfind(']')]
  return version

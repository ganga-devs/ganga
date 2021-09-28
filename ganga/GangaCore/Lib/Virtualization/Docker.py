##########################################################################
# Ganga Project. https://github.com/ganga-devs/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization


class Docker(IVirtualization):

    """
    The job will be run inside a container using Docker or UDocker as the virtualization method. Docker
    is tried first and if not installed or permission do not allow it, UDocker is installed and used.

    j=Job()
    j.virtualization = Docker("fedora:latest")   

    The mode of the UDocker running can be modified. The P1 mode is working almost everywhere but might
    not give the best performance. See https://github.com/indigo-dc/udocker for more details about
    Udocker.

    If the image is a private image, the username and password of the deploy token can be given like

    j.virtualization.tokenuser = 'gitlab+deploy-token-123'
    j.virtualization.tokenpassword = 'gftrh84dgel-245^ghHH'

    Note that images stored in a docker repository hosted by Github at present doesn't work with uDocker 
    as uDocker is not updated to the latest version of the API.

    Directories can be mounted from the host to the container using key-value pairs to the mounts option.

    j.virtualization.mounts = {'/cvmfs':'/cvmfs'}
    """
    _name = 'Docker'
    _schema = IVirtualization._schema.inherit_copy()
    _schema.datadict['mode'] = SimpleItem(defvalue="P1", doc='Mode of container execution')

    def __init__(self, image, mode):
        super().__init__(image)
        self.mode = mode

    def modify_script(self, script, sandbox=False):
        """Overides parent's modify_script function
            Arguments other than self:
               script - Script that need to be modified

            Return value: modified script"""

        extra = 'virtualization_image = ' + repr(self.image) + '\n'
        extra = extra + 'virtualization_user = ' + repr(self.tokenuser) + '\n'
        extra = extra + 'virtualization_password = ' + repr(self.tokenpassword) + '\n'
        extra = extra + 'virtualization_mounts = ' + repr(self.mounts) + '\n'
        extra = extra + 'virtualization_options = ' + repr(self.options) + '\n'

        if sandbox:
            extra = extra + 'virtualization_udockerlocation = ' + repr(getcwd())
            extra = extra + 'runenv[\'UDOCKER_DIR\']=' + repr(path.join(getcwd(),'.udocker'))
        else:
            extra = extra + 'virtualization_udockerlocation = ' + \
            repr(getConfig('Configuration')['UDockerlocation'])  + '\n'
        
        extra = extra + """

from Virtualization import checkDocker, checkUDocker, checkSingularity, installUdocker
options = []
                
if execmd[0].startswith('./'): 
    execmd[0] = "/work_dir/"+execmd[0]
if (checkDocker()):
    print("Using Docker")
    if virtualization_user:
        buildcommand = ['docker', 'login', '--username='+virtualization_user, '--password='+virtualization_password]
        rc = subprocess.call(buildcommand, env=runenv, shell=False)
    for k,v in virtualization_mounts.items():
        if os.path.isdir(k):
            options = options + ['-v' , k + ':' + v]
        else:
            print('Requested directory %s is not available and no bind will be made to container' % k)
    options = options + virtualization_options
    execmd = ['docker', 'run', '--rm', '-v', workdir+":"+"/work_dir"] + options + [virtualization_image] + execmd        
else:
    print("Docker not available or no permission to run docker demon, will attempt UDocker.")
    location = os.path.expanduser(virtualization_udockerlocation)
    binary = os.path.join(location,'udocker')
    if not (checkUDocker(location)):
        try:
            installUdocker(location)
        except OSError as x:
            failurereport(statusfile, 'PROBLEM WITH UDOCKER: %s' % str(x))
    runenv["PROOT_NO_SECCOMP"]="1"
    runenv['UDOCKER_DIR']=os.path.join(location,'.udocker')
    if virtualization_user:
        buildcommand = [binary, 'login', '--username='+virtualization_user, '--password='+virtualization_password]
        rc = subprocess.call(buildcommand, env=runenv, shell=False)
    for k,v in virtualization_mounts.items():
        if os.path.isdir(k):
            options = options + ['--volume='+ k + ':' + v]
        else:
            print('Requested directory %s is not available and no bind will be made to container' % k)
    options = options + virtualization_options
    execmd = [binary, '--quiet', 'run', '--rm', '--volume', workdir+":"+"/work_dir"] + options + [virtualization_image] + execmd

"""
        script = script.replace('###VIRTUALIZATION###',extra)
        return script

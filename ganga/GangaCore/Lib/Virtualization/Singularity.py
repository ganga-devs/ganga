##########################################################################
# Ganga Project. https://github.com/ganga-devs/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile

class Singularity(IVirtualization):

    """
    The Singularity class can be used for either Singularity or Docker images. 
    It requires that singularity is installed on the worker node.

    For Singularity images you provide the image name and tag from Singularity 
    hub like

      j=Job()
      j.application=Executable(exe=File('my/full/path/to/executable'))
      j.virtualization = Singularity("shub://image:tag")

    Notice how the executable is given as a `File` object. This ensures that it 
    is copied to the working directory and thus will be accessible inside the 
    container.

    The container can also be provided as a Docker image from a repository. The 
    default repository is Docker hub.

      j.virtualization = Singularity("docker://gitlab-registry.cern.ch/lhcb-core/lbdocker/centos7-build:v3")

      j.virtualization = Singularity("docker://fedora:latest")   

    Another option is to provide a `GangaFile` Object which points to a 
    singularity file. In that case the singularity image file will be copied to 
    the worker node. The first example is with an image located on some shared 
    disk. This will be effective for running on a local backend or a batch 
    system with a shared disk system.

      imagefile = SharedFile('myimage.sif', locations=['/my/full/path/myimage.sif'])
      j.virtualization = Singularity(image= imagefile)

    while a second example is with an image located in the Dirac Storage 
    Element. This will be effective when using the Dirac backend.

      imagefile = DiracFile('myimage.sif', lfn=['/some/lfn/path'])
      j.virtualization = Singularity(image= imagefile)

    If the image is a private image, the username and password of the deploy 
    token can be given like the example below. Look inside Gitlab setting for 
    how to set this up. The token will only need access to the images and 
    nothing else.

      j.virtualization.tokenuser = 'gitlab+deploy-token-123'
      j.virtualization.tokenpassword = 'gftrh84dgel-245^ghHH'

    Directories can be mounted from the host to the container using key-value 
    pairs to the mounts option. If the directory is not available on the host, a 
    warning will be written to stderr of the job and no mount will be attempted.

      j.virtualization.mounts = {'/cvmfs':'/cvmfs'}

    By default the container is started in singularity with the `--nohome` 
    option. Extra options can be provided through the `options` attribute. See 
    the Singularity documentation for what is possible.

    If the singularity binary is not available in the PATH on the remote node - or has a different name, 
    it is possible to give the name of it like

      j.virtualization.binary='/cvmfs/oasis.opensciencegrid.org/mis/singularity/current/bin/singularity'

    """
    _name = 'Singularity'
    _schema = IVirtualization._schema.inherit_copy()
    _schema.datadict['image'] = SimpleItem(defvalue="",
                                           typelist=[str,'GangaCore.GPIDev.Adapters.IGangaFile.IGangaFile'],
                                           doc='Link to the container image. This can either be a singularity URL or a GangaFile object')
    _schema.datadict['binary'] = SimpleItem(defvalue="singularity",
                                            typelist=[str],
                                            doc='The virtualization binary itself. Can be an absolute path if required.')
    
    def modify_script(self, script, sandbox=False):
        """Overides parent's modify_script function
                    Arguments other than self:
                       script - Script that need to be modified

                    Return value: modified script"""
        
        if isinstance(self.image, IGangaFile):
            extra = 'virtualization_image = ' + repr(self.image.namePattern) + '\n'
        else:
            extra = 'virtualization_image = ' + repr(self.image) + '\n'
        extra = extra + 'virtualization_user = ' + repr(self.tokenuser) + '\n'
        extra = extra + 'virtualization_password = ' + repr(self.tokenpassword) + '\n'
        extra = extra + 'virtualization_mounts = ' + repr(self.mounts) + '\n'
        extra = extra + 'virtualization_options = ' + repr(self.options) + '\n'
        extra = extra + 'virtualization_binary = ' + repr(self.binary) + '\n'

        extra = extra + """
print("Using singularity")

import stat
if not ( ('XDG_RUNTIME_DIR' in runenv) and
         os.path.isdir(runenv['XDG_RUNTIME_DIR']) and
         (stat.S_IMODE(os.stat(runenv['XDG_RUNTIME_DIR']).st_mode) == 0o700) and
         os.access(runenv['XDG_RUNTIME_DIR'], os.W_OK) ):
    os.mkdir('.xdg', 0o700)
    runenv['XDG_RUNTIME_DIR'] = os.path.join(os.getcwd(), '.xdg')
           
options = []
if virtualization_user:
    runenv["SINGULARITY_DOCKER_USERNAME"] = virtualization_user
    runenv["SINGULARITY_DOCKER_PASSWORD"] = virtualization_password
for k,v in virtualization_mounts.items():
    if os.path.isdir(k):
        options = options + ['--bind' , k + ':' + v]
    else:
        print('Requested directory %s is not available and no bind will be made to container' % k)
options = options + virtualization_options
if execmd[0].startswith('./'): 
    execmd[0] = "/work_dir/"+execmd[0]
"""

        if sandbox:
            extra = extra + """
runenv['SINGULARITY_CACHEDIR']=os.path.join(os.getcwd(),'.singularity','cache')
for i in range(3):
    try:
        buildcommand = [virtualization_binary, 'build', '--sandbox', 'singularity_sandbox' , virtualization_image]
        rc = subprocess.call(buildcommand, env=runenv, shell=False)
        if rc==0:
            break
    except Exception as x:
        print('Exception occured in downloading Singularity image: ' + str(buildcommand))
        print('Err was: ' + str(x))
execmd = [virtualization_binary, '-q', 'exec', '--bind', 
          workdir+":"+"/work_dir", "--no-home"] + options + ['singularity_sandbox'] + execmd   
"""
        else:
            extra = extra + """
execmd = [virtualization_binary, '-q', 'exec', '--bind',
          workdir+":"+"/work_dir", "--no-home"] + options + [virtualization_image] + execmd   

"""
        script = script.replace('###VIRTUALIZATION###',extra)
        return script

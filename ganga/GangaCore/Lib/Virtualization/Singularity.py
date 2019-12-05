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

      j.virtualization = Docker("docker://fedora:latest")   

    Another option is to provide a `GangaFile` Object which points to a 
    singularity file. In that case the singularity image file will be copied to 
    the worker node. The first example is with an image located on some shared 
    disk. This will be effective for running on a local backend or a batch 
    system with a shared disk system.

      imagefile = SharedFile('myimage.sif', locations=['/my/full/path/myimage.sif'])
      j.virtualization = Singularity(image= imagefile)

    while a second example is with an image located in he Dirac Storage 
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
    pairs to the mount option. If the directory is not vailable on the host, a 
    warning will be written to stderr of the job and no mount will be attempted.

      j.virtualization.mount = {'/cvmfs':'/cvmfs'}

    By default the container is started in singularity with the `--nohome` 
    option. Extra options can be provided through the `options` attribute. See 
    the Singularity documentation for what is possible.
    """
    _name = 'Singularity'
    _schema = IVirtualization._schema.inherit_copy()
    _schema.datadict['image'] = SimpleItem(defvalue="",
                                           typelist=[str,'GangaCore.GPIDev.Adapters.IGangaFile.IGangaFile'],
                                           doc='Link to the container image. This can either be a singularity URL or a GangaFile object')

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

        extra = extra + """
print("Using singularity")
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
runenv['SINGULARITY_CACHEDIR']=path.join(getcwd(),'.singularity','cache')
for i in range(3):
    try:
        buildcommand = ['singularity', 'build', '--sandbox', 'singularity_sandbox' , virtualization_image]
        rc = subprocess.call(buildcommand, env=runenv, shell=False)
        if rc==0:
            break
    except Exception as x:
        print('Exception occured in downloading Singularity image: ' + str(buildcommand))
        print('Err was: ' + str(x))
execmd = ['singularity', '-q', 'exec', '--bind', 
          workdir+":"+"/work_dir", "--no-home"] + options + ['singularity_sandbox'] + execmd   
"""
        else:
            extra = extra + """
execmd = ['singularity', '-q', 'exec', '--bind',
          workdir+":"+"/work_dir", "--no-home"] + options + [virtualization_image] + execmd   

"""
        script = script.replace('###VIRTUALIZATION###',extra)
        return script

##########################################################################
# Ganga Project. https://github.com/ganga-devs/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile

class Singularity(IVirtualization):

    """
    The job will be run inside a container using singularity as the virtualization method.

    j=Job()
    j.virtualization = Singularity("shub://image:tag")
  
    or it can be used with a Docker image

    j.virtualization = Singularity("docker://gitlab-registry.cern.ch/lhcb-core/lbdocker/centos7-build:v3")

    j.virtualization = Docker("docker://fedora:latest")   

    or you can provide a GangaFile Object which points to a singularity file as shown below,

    In that case the singularity image file will be copied to the worker node.
    imagefile = LocalFile("path_to_image.sif")
    j.virtualization = Singularity(image= imagefile)
    j.inputfiles = j.inputfiles + [imagefile]

    If the image is a private image, the username and password of the deploy token can be given like

    j.virtualization.tokenuser = 'gitlab+deploy-token-123'
    j.virtualization.tokenpassword = 'gftrh84dgel-245^ghHH'

    Directories can be mounted from the host to the container using key-value pairs to the mount option.

    j.virtualization.mount = {'/cvmfs':'/cvmfs'}

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

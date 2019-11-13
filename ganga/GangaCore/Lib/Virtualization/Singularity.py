##########################################################################
# Ganga Project. http://cern.ch/ganga
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

    or you can provide a GangaFile Object which points to a singularity file as shown below,

    In that case the singularity image file will be copied to the worker node.
    imagefile = LocalFile("path_to_image.sif")
    j.virtualization = Singularity(image= imagefile)
    j.inputfiles = j.inputfiles + [imagefile]
    """
    _name = 'Singularity'
    _schema = IVirtualization._schema.inherit_copy()
    _schema.datadict['image'] = SimpleItem(defvalue="",
                                           typelist=[str,'GangaCore.GPIDev.Adapters.IGangaFile.IGangaFile'],
                                           doc='Link to the container image')

    def modify_script(self, script):
        """Overides parent's modify_script function
                    Arguments other than self:
                       script - Script that need to be modified

                    Return value: modified script"""
        
        if isinstance(self.image, IGangaFile):
            script = script.replace('###VIRTUALIZATIONIMAGE###', repr(self.image.namePattern))
        script = script.replace('###VIRTUALIZATIONIMAGE###', repr(self.image))
        script = script.replace('###VIRTUALIZATION###', repr("Singularity"))
        script = script.replace('###VIRTUALIZATIONMODE###', repr(None))
        return script

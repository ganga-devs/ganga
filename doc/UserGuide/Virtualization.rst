
Virtualization
==============
It is possible to run a Ganga job inside a container. This allows you to get a completely well defined environment on the worker node where the job is executed. Each job has a virtualization attribute which defines the image to be used for the container as a required attribute. Images can be either from Docker or Singularity hub, from the images created by Gitlab or in case of Singularity from a file provided as a GangaFile.

Using images can provide an attractive workflow where GitLab continuous integration is used to create Docker images. Those images can then subsequently be used for running jobs where it is assured that they are in the same environment. The image can either be used directly from the repository (using the deploy username/password if private) or can be pulled and converted to a Singularity image.

Try it out
----------
.. code-block:: python

    j1 = Job(name='Weather', \
             virtualization=Docker(image='uegede/weather'), \
             application=Executable(exe='weather', args=['MEL']))
    j2 = Job(name='Fedora', \
             virtualization=Docker(image='fedora:latest'), \
             application=Executable(exe='cat', args=['/etc/redhat-release']))

.. image:: virtualization.gif
  :width: 915
  :alt: Animation of jobs running inside containers
	     
Singularity class
-----------------
The Singularity class can be used for either Singularity or Docker images. It requires that singularity is installed on the worker node.

For Singularity images you provide the image name and tag from Singularity hub like

.. code-block:: python

    j=Job()
    j.application=Executable(exe=File('my/full/path/to/executable'))
    j.virtualization = Singularity("shub://image:tag")

Notice how the executable is given as a ``File`` object. This ensures that it is copied to the working directory and thus will be accessible inside the container.
  
The container can also be provided as a Docker image from a repository. The default repository is Docker hub. 

.. code-block:: python

    j.virtualization = Singularity("docker://gitlab-registry.cern.ch/lhcb-core/lbdocker/centos7-build:v3")
    j.virtualization = Docker("docker://fedora:latest")   

Another option is to provide a ``GangaFile`` Object which points to a singularity file. In that case the singularity image file will be copied to the worker node. The first example is with an image located on some shared disk. This will be effective for running on a local backend or a batch system with a shared disk system.

.. code-block:: python

    imagefile = SharedFile('myimage.sif', locations=['/my/full/path/myimage.sif'])
    j.virtualization = Singularity(image= imagefile)

while a second example is with an image located in the Dirac Storage Element. This will be effective when using the Dirac backend.

.. code-block:: python

    imagefile = DiracFile('myimage.sif', lfn=['/some/lfn/path'])
    j.virtualization = Singularity(image= imagefile)
  
If the image is a private image, the username and password of the deploy token can be given like the example below. Look inside Gitlab setting for how to set this up. The token will only need access to the images and nothing else.

.. code-block:: python

    j.virtualization.tokenuser = 'gitlab+deploy-token-123'
    j.virtualization.tokenpassword = 'gftrh84dgel-245^ghHH'

Directories can be mounted from the host to the container using key-value pairs to the mounts option. If the directory is not vailable on the host, a warning will be written to stderr of the job and no mount will be attempted.

.. code-block:: python

    j.virtualization.mounts = {'/cvmfs':'/cvmfs'}

By default the container is started in singularity with the ``--nohome`` option. Extra options can be provided through the ``options`` attribute. See the Singularity documentation for what is possible.

Docker class
------------
You can define a docker container by providing an image name and tag. Using that ganga will fetch 
the image from the docker hub. 

.. code-block:: python

    j=Job()
    j.virtualization = Docker("image:tag")

Ganga will try to run the container using Docker if Docker is availabe in the worker node and if the user has the 
permission to run docker containers. If not ganga will download `UDocker <https://github.com/indigo-dc/udocker>`_ which provides the ability to run docker containers in userspace. The runmode in Udocker can be changed as seen in the documentation. Using Singualarity as the run mode is not recommended; use the ``Singularity`` class above instead.

Issues to keep in mind
----------------------

Awareness should be given to the load that using containers will impose on the system where they are running

* If the file system is shared (like for the ``Batch`` and ``Local`` backends, the images pulled down from a remote repository will be cached locally.
* If the file system is not shared (like for the ``LCG`` and ``Dirac`` backends), then images from remote repositories will be pulled for each job. This might put an excessive load on the network and/or the repository.
* If the image for ``Singularity`` is given as a file, it will be copied to the worker node. If provided as a ``DiracFile`` object, it can be replicated to the sites where the job will be asked to run to limit the impact of pulling the image.


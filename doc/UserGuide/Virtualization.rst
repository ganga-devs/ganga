
Ganga Virtualization
=====================
Ganga also lets you to run your job on your own docker/singularity container. This gives you the ability to define your 
own environment in which your task need to be executed.

Using Docker Container
----------------------
You can define a docker container by providing the image name and tag as shown below. Using that ganga will fetch 
the image from the docker hub. 
::
  j=Job()
  j.virtualization = Docker("image:tag")`

Ganga will try to run the container using Docker if Docker is availabe in the worker node and if the user has the 
permission to run docker containers. If not ganga will download `UDocker <https://github.com/indigo-dc/udocker>`_ which provides the ability to run docker 
containers in userspace. 

Using Singularity Container
-----------------------------
For Singularity images you provide the image name and tag as follow,
::
  j=Job()
  j.virtualization = Singularity("shub://image:tag")`
  
or you can provide a GangaFile Object which points to a simg file as shown below,
In that case the simg image file will be copied to the worker node.
::
  j=Job()
  j.virtualization = Singularity(LocalFile("path_to_image.simg"))`



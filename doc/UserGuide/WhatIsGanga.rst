What is Ganga
=============

Many scientific computations are too large to take care of by simply running a script from the command line and waiting for it to execute. To get around this many different systems has been used over the years

* Starting tasks in the background on your desktop;
* Using a local batch system or batch system on a central facility;
* Using various grid or cloud systems for submitting your code to.

Getting your work done like this often means that it gets broken into multiple pieces. All these pieces are both tedious and error prone and include things like:

* Ensure that you use same code for testing locally and running on grid system;
* Pack up ancillary files that are required for running on remote system;
* Split your task up into many smaller pieces;
* Submit each of the smaller pieces, keep track of which fail, resubmit them;
* Keep running commands to see if all the jobs have finished;
* Merge all the pieces together.

The idea in Ganga is to take all these problems, provide a Python API for them that allows them to be solved in a clean and programmatic way. In addition, Ganga will provide a service that takes care of monitoring the progress of all tasks that have been submitted. This means that a workflow using Ganga is more like.

* Create your task inside Ganga and test it locally on your desktop;
* Specify to Ganga how you want you task broken up and the results merged;
* Tell Ganga where your task should be executed (Batch, Grid, ...) and submit it;
* Let Ganga monitor the progress, resubmit failed pieces and merge the results in the end.

Ganga provides a plugin system that allows groups such as HEP collaborations to expand the API with specific applications that will make it easier to run tasks on remote systems (build shared libraries, find configuration files, interact with data bookkeeping). There is also support for running taks inside docker and singularity containers.

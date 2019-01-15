Tasks
=====

Introduction to Tasks
---------------------

Even with Ganga, you can find that you may find managing a large set of jobs and steps in an analysis to be a bit
cumbersome. The GangaTasks package was developed to help with these larger scale analyses and remove as much of the
'busy work' as possible. It can automatically submit jobs to keep a set number running, it can create new jobs when
others complete and chain their data together, it can automatically transfer data around as required and a number of
other things as well. As with all of Ganga it is based on the plugin system and so you can easily extend some elements
of it to better suit your requirements.

GangaTasks essentially adds 3 new objects that control all aspects of the overall task:

* Task
    This is overall 'container' for the steps in your analysis. It is fairly light weight but is used to aggregate
    the overall status of the task and control overall settings, numbers of jobs, etc.
* Transform
    This is where most things occur. It is in some ways analogous to a Job Template in that it mostly contains the
    objects that will be assigned to the created jobs. This is where new Units are created and data is transferred
    between steps. You will generally have a Transform per 'step' or 'type' of job that you want to run.
* Unit
    This is the 'control class' for any created jobs and contains all the job-specific information (e.g. input data,
    application settings, etc.) that each actual Job will be setup with. After all the units in a Transform are
    created, each unit then creates a new Job and attempts to submit it. It will monitor the status of the job and
    will do any necessary actions (e.g. download output data) upon completion. If the job fails and it seems sensible
    to do so, it will also resubmit or recreate the job.

A typical example of how this structure works would be in a two stage analysis where you generate some MC in the first
step and then run some analysis code on the output of this data. You would create an overall Task to manage both steps.
Each step would have an associated Transform with the first being setup as MC generation and the second doing the
analysis. You would set the input data of the second transform to be the output data of the first. Then, while running
your Task, Units will be created to cover the number of events you wanted to create and jobs will be submitted for
each of these. As these complete new units and jobs will be created by the analysis Transform to cover that step.

Basic Core Usage
----------------

It's quite likely you will want to develop your own plugins to maximise your use of GangaTasks, however there is a set
of generalised classes that can get you started. Typical use of these is shown below:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- TASKS EXAMPLE START
    :end-before: # -- TASKS EXAMPLE STOP
    :dedent: 8


After running the above commands you won't see much happen initially as Tasks runs on a separate monitoring loop that
triggers every 30s (configurable in ``~/.gangarc``). Eventually though you will see the units created and then jobs
for each of these units will be submitted. To see the progress of your tasks use:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- TASKS OVERVIEW START
    :end-before: # -- TASKS OVERVIEW STOP
    :dedent: 8


Tasks can also take advantage of using queues for submission as well. Simply add:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- TASKS OPTIONS START
    :end-before: # -- TASKS OPTIONS STOP
    :dedent: 8


Job Chaining
------------

The Tasks package also allows you to chain jobs together, i.e. have the output of one job be the input of another.
This is done by setting the input data of the dependant Transform to be ``TaskChainInput`` type and giving the ID of
the Transform is depends on. You can have multiple transforms feed into one Transform by specifying more
``TaskChainInput`` datasets.

A typical example is shown below:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- TASKS JOBCHAIN START
    :end-before: # -- TASKS JOBCHAIN STOP
    :dedent: 8


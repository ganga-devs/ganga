Job Manipulation
================

There are several ways to control and manipulate your jobs within Ganga.

Copying Jobs
------------

You can copy jobs using the ``copy`` method or using the cop-constructor in the Job creation. The job status is
always set to ``new``:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION JOBCOPY START
    :end-before: # -- JOBMANIPULATION JOBCOPY STOP
    :dedent: 8

.. code-block:: python

    Ganga Out [3]:
    Registry Slice: jobs (4 objects)
    --------------
        fqid |    status |      name | subjobs |    application |        backend |                             backend.actualCE |                       comment
    -------------------------------------------------------------------------------------------------------------------------------------------------------------
           0 | completed |           |         |     Executable |          Local |                       epldt017.ph.bham.ac.uk |
           1 | completed |  original |         |     Executable |          Local |                       epldt017.ph.bham.ac.uk |
           2 |       new |      copy |         |     Executable |          Local |                                              |
           3 |       new |     copy2 |         |     Executable |          Local |                                              |


Accessing Jobs in the Repository
--------------------------------

As shown before, you can view all the jobs that Ganga is aware of using the ``jobs`` command. To access a specific
job from the repo with the parentheses, use it's ``id`` number or:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION REPOACCESS START
    :end-before: # -- JOBMANIPULATION REPOACCESS STOP
    :dedent: 8

You can also use the square bracket (``[]``) notation to specify single jobs, lists of jobs or a job by a (unique) name:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION JOBSLICING START
    :end-before: # -- JOBMANIPULATION JOBSLICING STOP
    :dedent: 8

Resubmitting Jobs
-----------------

Jobs can fail for any number of reasons and often it's a transient problem that resubmitting the job will solve.
To do this in Ganga, simply call the ``resubmit`` method:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION RESUBMIT START
    :end-before: # -- JOBMANIPULATION RESUBMIT STOP
    :dedent: 8

Note that, as above, this can also be used on ``completed`` jobs, though it's backend and application dependent.

Forcing to Failed
-----------------

Sometimes you may encounter a problem where the job has been marked ``completed`` by the backend but you notice in
the logs that there was a problem which renders the output useless. To mark this job as ``failed``, you can do:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION FORCESTATUS START
    :end-before: # -- JOBMANIPULATION FORCESTATUS STOP
    :dedent: 8

Note that there are PostProcessors in Ganga that can help with a lot of these kind of problems.

Removing Jobs
-------------

As you submit more jobs, your Ganga repository will grow and could become quite large. If you have finished with
jobs it is good practise to remove them from the repository:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION JOBREMOVE START
    :end-before: # -- JOBMANIPULATION JOBREMOVE STOP
    :dedent: 8

This will remove all associated files and directories from disk.

Performing Bulk Job Operations
------------------------------

There are several job operations you can perform in bulk from a set of jobs. To obtain a list of jobs, you can
either use the array syntax described above or the ``select`` method:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION JOBSELECT START
    :end-before: # -- JOBMANIPULATION JOBSELECT STOP
    :dedent: 8

Given this selection, you can then perform a number of operations on all of the jobs at once:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION JOBSELECTOP START
    :end-before: # -- JOBMANIPULATION JOBSELECTOP STOP
    :dedent: 8

Available operations are: ``submit``, ``copy``, ``kill``, ``resubmit``, ``remove``. These also take the ``keep_going``
argument which, if set to ``True`` will mean that it will keep looping through the jobs even if an error occurs
performing the operation on one of them. These operations can also be performed on subjobs as well - see
:doc:`SplittersAndPostprocessors` for more info.

Export and Import of Ganga Objects
----------------------------------

Ganga is able to export a Job object (or a ``selection`` of Job objects) or any other Ganga object using the ``export``
method which will create a human readable text file that you can edit manually and then load in using ``load``:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- JOBMANIPULATION EXPORTJOB START
    :end-before: # -- JOBMANIPULATION EXPORTJOB STOP
    :dedent: 8

As in the above example, any jobs loaded will be put into the ``new`` state.

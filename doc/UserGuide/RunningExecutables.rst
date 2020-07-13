Running Executables
===================

You can run any executable or script through Ganga using the ``Executable`` application. This accepts either a full
path to an already installed exe (e.g. on CVMFS) or a Ganga ``File`` object that will be sent with your job. You
can also supply arguments and environment settings with the options in the ``Executable`` object.

As an example:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- RUNNINGEXECUTABLES EXAMPLE START
    :end-before: # -- RUNNINGEXECUTABLES EXAMPLE STOP
    :dedent: 8

If your executable requires more than one file to run, you can use the ``inputfiles`` field of the ``Job`` object
to send this across as well (see :doc:`InputAndOutputData`).

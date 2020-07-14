Using Different Backends
========================

One of the main benefits of Ganga is that you can submit to different clusters/systems (in Ganga, these are termed
backends) by only changing one or two lines in your scripts. Though there are often very different ways of submission for each backend, Ganga tries to hide this as much as possible and follow a submission that is more or less identical to what is done for a Local job.

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- USINGDIFFERENTBACKENDS PLUGINS START
    :end-before: # -- USINGDIFFERENTBACKENDS PLUGINS STOP
    :dedent: 8

Local Backend
-------------

This is the default and refers to the machine that Ganga is running on. The job will be spawned as a separate process, independent of Ganga. Typical usage is:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- USINGDIFFERENTBACKENDS LOCAL START
    :end-before: # -- USINGDIFFERENTBACKENDS LOCAL STOP
    :dedent: 8

There are no editable options for the object itself but there are two config options that you can view with
``config.Local``. You can quit Ganga and restart and the Local job will still run in the background.

Batch Backends
--------------

Ganga supplies backend objects for most of the major batch systems around - Condor, Slurm, SGE, LSF and PBS. You should obviously use the one that is relevant to the system you are running on. Typical usage is detailed below though as with all these, you can get more help using ``help(<backend>)`` and ``config.<backend>``. Sometimes a local installation requires that small changes are made to the configuration. Look in the relevant section of the ``~/.gangarc`` file.

LSF
^^^^

.. code-block:: python

    j = Job()
    j.backend = LSF()
    j.backend.queue = '1nh'

Slurm
^^^^^

Very similar to the LSF backend, this is setup by default to submit to a typical Slurm installation but again,
can easily be changed to reflect your specific setup:

.. code-block:: python

    j = Job()
    j.backend = Slurm()
    j.submit()

Condor
^^^^^^

Condor is a little different than the other backends but should still submit to most typical installations. There is
also a requirements object that can be used to specify memory, architecture, etc.

.. code-block:: python

    j = Job()
    j.backend = Condor()
    j.backend.getenv = "True"  # send the environment to the host
    j.backend.requirements.memory = 1200
    j.submit()

Also note that the ``getenv`` option is defined as a string so in your ``.gangarc``, you would need to set it to:

.. code-block:: python

    [Condor]
    getenv = 'True'

To avoid Ganga attempting to assign a boolean instead.

Dirac Backend
-------------

To submit to a Dirac instance, you will need to have the Dirac client installed and a Dirac proxy available.

Using GridPP Dirac on CVMFS
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is an installed version of Dirac configured to use the GridPP Dirac instance available on the Ganga CVMFS area. To run with this, do

.. code-block:: bash

    /cvmfs/ganga.cern.ch/runGanga-dirac.sh

A few questions about your Virtual Organisation will be asked the first time you run.

    
Installing and Configuring the Dirac Client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are not using the GridPP instance of Dirac, or don't have access to CVMFS then you will need to install and configure the Dirac client. See `here <https://dirac.readthedocs.io/en/latest/UserGuide/GettingStarted/InstallingClient/>`_ for instructions.

After successfully doing this, do the following steps to configure Ganga:

* Edit your ``.gangarc`` file and set the following options:

.. code-block:: python

    [Configuration]
    RUNTIME_PATH = GangaDirac

    [DIRAC]
    DiracEnvSource = /home/<username>/dirac_ui/bashrc

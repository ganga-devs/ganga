Using Different Backends
========================

One of the main benefits of Ganga is that you can submit to different clusters/systems (in Ganga, these are termed
backends) by only changing one or two lines in your scripts. Though there are often very different ways of submission
for each backend, Ganga tries to hide this as much as possible. We'll cover the main options in Ganga Core below but
to get a full list, use:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- USINGDIFFERENTBACKENDS PLUGINS START
    :end-before: # -- USINGDIFFERENTBACKENDS PLUGINS STOP
    :dedent: 8

Local Backend
-------------

This is the default and refers to the machine that Ganga is running on. The job will be spawned as a separate process,
independent of Ganga. Typical usage is:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- USINGDIFFERENTBACKENDS LOCAL START
    :end-before: # -- USINGDIFFERENTBACKENDS LOCAL STOP
    :dedent: 8

There are no editable options for the object itself but there are two config options that you can view with
``config.Local``.

Batch Backends
--------------

Ganga supplies backend objects for most of the major batch systems around - LSF, PBS and  Condor. You should
obviously use the one that is relevant to the system you are running on. Typical usage is detailed below though
as with all these, you can get more help using ``help(<backend>)`` and ``config.<backend>``:

LSF
^^^^

The default setup will work for LSF set up at CERN but you can change the patterns searched for in the output of the LSF commands if these are different in your particular setup.

.. code-block:: python

    j = Job()
    j.backend = LSF()
    j.backend.queue = '1nh'

PBS
^^^^

Very similar to the LSF backend, this is setup by default to submit to a typical Torque/Maui installation but again,
can easily be changed to reflect your specific setup:

.. code-block:: python

    j = Job()
    j.backend = PBS()
    j.submit()

Condor
^^^^^^

Condor is a little different than the other backends but should still submit to most typical installations. There is
also a requirements object that can be used to specify memory, architecture, etc.

.. code-block:: python

    j = Job()
    j.backend = Condor()
    j.backend.getenv = True  # send the environment to the host
    j.backend.requirements.memory = 1200
    j.submit()

Dirac Backend
-------------

To submit to a Dirac instance, you will need to have the Dirac client installed and a Dirac proxy available.

Using GridPP Dirac on CVMFS
^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is an installed version of Dirac configured to use the GridPP Dirac instance available on the Ganga CVMFS area.
To run with this, simply do the following:

* If you are not running with on the GridPP VO, change the following in your ``~/.gangarc`` file:

.. code-block:: python

    [defaults_GridCommand]
    init = dirac-proxy-init -g <dirac user group> -M

* After this, you can run Ganga with the Dirac plugins setup and pointing to the GridPP Dirac instance by simply running:

.. code-block:: bash

    /cvmfs/ganga.cern.ch/runGanga-dirac.sh


Installing and Configuring the Dirac Client
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If don't have access to CVMFS then you will need to install and configure the Dirac client. How this
is done will depend somewhat on your Dirac instance but for the one hosted by GridPP, first follow the
installation instructions `here <https://www.gridpp.ac.uk/wiki/Quick_Guide_to_Dirac>`_

After successfully doing this, do the following steps to configure Ganga:

* Edit your ``.gangarc`` file and set the following options:

.. code-block:: python

    [Configuration]
    RUNTIME_PATH = GangaDirac

    [LCG]
    GLITE_ENABLE = True
    GLITE_SETUP = /home/<username>/dirac_ui/bashrc

    [DIRAC]
    DiracEnvSource = /home/<username>/dirac_ui/bashrc

    [defaults_GridCommand]
    info = dirac-proxy-info
    init = dirac-proxy-init -g <dirac user group> -M


Testing Dirac Submission Through Ganga
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To test everything is working, run Ganga (either using the CMVFS helper script ``runGanga-dirac.sh`` or as normal).
It should ask you to generate a proxy if you haven't already and then leave you at the IPython prompt.

To test that all is working, try to submit a basic job to the local machine you're running and then to DIRAC:

.. code-block:: bash

    j = Job()
    j.backend = Dirac()
    j.submit()

If you go to the Dirac portal (for GridPP, go `here <https://dirac.gridpp.ac.uk/DIRAC>`_), you should now see your job!
On completion, you can view the output of the job using:

.. code-block:: bash

    j.peek('Ganga_Executable.log')


LCG Backends
------------

There are several ways to submit 'directly' to the LCG grid through Ganga that are detailed below. However, all these
require that you have a Grid UI installed and a Grid Proxy available. Ganga will check if you have a proxy when it
needs it and prompt to create one if it's not available. To set your config correctly, use the following:

```
```

#### LCG backend

#### ARC backend

#### CREAM backend

## Remote Backend

This backend allows you to submit a job 'through' a gateway host. IT IS CURRENTLY BEING FIXED!!!
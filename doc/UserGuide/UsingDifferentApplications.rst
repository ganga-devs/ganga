Using Different Applications
============================

For executing some complicated computation, of ten more than just a simple executable is provided. Maybe certain calibration files are required as well, compilation of code is required first that gives rise to shared libraries and so on. Ganga allows through a plugin system for specific applications to be executed. You can see which ones are available as

.. code-block:: python

    Ganga In [1]: plugins('applications')
    Ganga Out [1]: ['Executable', 'Root', 'Notebook', 'PrimeFactorizer']

Large Particle Physics collaborations, such as LHCb and T2K have specific applications written and it is quite easy to write your own. 

Try it out
----------
The ``PrimeFactorizer`` application is part of the Tutorial plugin (:doc:`TutorialPlugin`). It illustrates how a specific application can be used instead of the default `Executable` application. You can try and create a job like

.. code-block:: python

    j = Job(application = PrimeFactorizer(number=1527), inputdata = PrimeTableDataset())

and submit it. In the output of the job when finished you should see the prime factors of the number. For the `inputdata` in the job specification above, see :doc:`InputAndOutputData`.


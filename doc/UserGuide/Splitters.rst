Splitters
=========

One of the main benefits of Ganga is it's ability to split a job description across many subjobs, changing the
input data or arguments appropriately for each. Ganga then keeps these subjobs organised with the parent master
job but keeps track of all their status, etc. individually. There are two main splitters that are provided in Ganga
Core which are detailed below.

GenericSplitter
---------------

The ``GenericSplitter`` is a useful tool to split a job based on arguments or parameters in an application or backend.
You can specify whatever attribute you want to split over within the job as a string using the ``attribute`` option.
A typical example using the basic ``Executable`` application is to produce subjobs with different arguments:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- SPLITTERS BASICUSE START
    :end-before: # -- SPLITTERS BASICUSE STOP
    :dedent: 8


This produces 3 subjobs with the arguments:

.. code-block:: bash

    echo hello 1    # subjob 1
    echo world 2    # subjob 2
    echo again 3    # subjob 3


Each subjob is essentially another ``Job`` object with all the parameters set appropriately for the subjob. You
can check each one by using:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- SPLITTERS SUBJOBCHECK START
    :end-before: # -- SPLITTERS SUBJOBCHECK STOP
    :dedent: 8


There may be times where you want to split over multiple sets of attributes though, for example the ``args`` and
the ``env`` options in the ``Executable`` application. This can be done with the ``multi_attrs`` option that takes
a dictionary with each key being the attribute values to change and the lists being the values to change. Give
the following a try:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- SPLITTERS MULTIATTRS START
    :end-before: # -- SPLITTERS MULTIATTRS STOP
    :dedent: 8


This will produce subjobs with the exe and environment:

.. code-block:: bash

    echo hello1 ; MYENV = test1  # subjob 1
    echo hello2 ; MYENV = test2  # subjob 2

GangaDatsetSplitter
-------------------

The ``GangaDatasetSplitter`` is provided as an easy way of splitting over a number input data files given in the
``inputdata`` field of a job. The splitter will create a subjob with the maximum number of file specified
(default is 5). A typical example is:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- SPLITTERS DATASETSPLITTER START
    :end-before: # -- SPLITTERS DATASETSPLITTER STOP
    :dedent: 8


If you check the output you will see the list of files that each subjob was given using ``j.subjobs()`` as above.

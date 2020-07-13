Splitters
=========

One of the main benefits of Ganga is it's ability to split a job description across many subjobs, changing the
input data or arguments appropriately for each. Ganga then keeps these subjobs organised with the parent master
job but keeps track of all their status, etc. individually. There are two main splitters that are provided in Ganga
Core which are detailed below. You can see which splitter are available with

.. code-block:: python
    Ganga In [1]: plugins('splitters')
    Ganga Out [1]: 
    ['ArgSplitter',
     'GenericSplitter',
     'GangaDatasetSplitter',
     'PrimeFactorizerSplitter']


Try it out:
-----------
Using the prime factorisation example from the Tutorial plugin (:doc:`TutorialPlugin`). We can split up the factorisation of a very large number up into 5 different tasks.

.. code-block:: python

    j = Job(application = PrimeFactorizer(number=268709474635016474894472456), \
            inputdata = PrimeTableDataset(table_id_lower=1, table_id_upper=30), \
            splitter = PrimeFactorizerSplitter(numsubjobs=10))

After the job and been submitted and finished, the output of each of the subjobs will be available. Remember that ganga is just a standard Python prompt, so we can use standard Python syntax

.. code-block:: python
    for js in j.subjobs: js.peek('stdout','cat')

See the section :doc:`PostProcessors` for how we can merge the output into a single file.

ArgSplitter:
------------

For a job that is using an `Executable` application, it is very common that you want to run it multiple times with a different set of arguments (like a random number seed). The `ArgSplitter` can do exactly that. For each of the subjobs created, it will replace the arguments fot he job with one from the array of array of arguments provided to the splitter. So

.. code-block:: python
    j = Job()
    j.splitter=ArgSplitter(args=[['Hello 1'], ['Hello 2']])

will create two subjobs where the ``Hello World`` of the default executable argument will be replaced by ``Hello 1`` and ``Hello 2`` respectively.


GenericSplitter
---------------

The ``GenericSplitter`` is a useful tool to split a job based on arguments or parameters in an application or backend. You can specify whatever attribute you want to split over within the job as a string using the ``attribute`` option. The example below illustrate how you can use it to do the same as the ``ArgSplitter``.

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
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

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- SPLITTERS SUBJOBCHECK START
    :end-before: # -- SPLITTERS SUBJOBCHECK STOP
    :dedent: 8


There may be times where you want to split over multiple sets of attributes though, for example the ``args`` and
the ``env`` options in the ``Executable`` application. This can be done with the ``multi_attrs`` option that takes
a dictionary with each key being the attribute values to change and the lists being the values to change. Give
the following a try:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- SPLITTERS MULTIATTRS START
    :end-before: # -- SPLITTERS MULTIATTRS STOP
    :dedent: 8


This will produce subjobs with the exe and environment:

.. code-block:: bash

    echo hello1 ; MYENV = test1  # subjob 1
    echo hello2 ; MYENV = test2  # subjob 2

GangaDatasetSplitter
-------------------

The ``GangaDatasetSplitter`` is provided as an easy way of splitting over a number input data files given in the
``inputdata`` field of a job. The splitter will create a subjob with the maximum number of file specified
(default is 5). A typical example is:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- SPLITTERS DATASETSPLITTER START
    :end-before: # -- SPLITTERS DATASETSPLITTER STOP
    :dedent: 8


If you check the output you will see the list of files that each subjob was given using ``j.subjobs()`` as above.

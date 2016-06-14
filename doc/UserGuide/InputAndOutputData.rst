Input And Output Data
=====================

Ganga tries to simplify sending input files and getting output files back as much as possible. You can specify
not only what files you want but where they should be retrieved/put. There are three fields that are relevant
for your job:

1. Input Files
    Files that are sent with the job and are available in the same directory on the worker node that runs it
2. Input Data
    A dataset or list of files that the job will run over but which are NOT transferred to the worker.
3. Output Files
    The name, type and location of the job output

Basic Input/Output File usage
-----------------------------

To start with, we'll show a job that sends an input text file with a job and then sends an output text file back:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA BASIC START
    :end-before: # -- INPUTANDOUTPUTDATA BASIC STOP
    :dedent: 8


After the job completes, you can then view the output directory and see the output file:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA PEEKOUTPUT START
    :end-before: # -- INPUTANDOUTPUTDATA PEEKOUTPUT STOP
    :dedent: 8


If the job doesn't produce the output Ganga was expecting, it will mark the job as failed:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA FAILJOB START
    :end-before: # -- INPUTANDOUTPUTDATA FAILJOB STOP
    :dedent: 8


You can also use wildcards in the files as well:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA WILDCARD START
    :end-before: # -- INPUTANDOUTPUTDATA WILDCARD STOP
    :dedent: 8


After completion, the output files found are copied as above but they are also recorded in the job appropriately:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA OUTPUTFILES START
    :end-before: # -- INPUTANDOUTPUTDATA OUTPUTFILES STOP
    :dedent: 8


This will also work for all backends as well - Ganga handles the changes in protocol behind the scenes, e.g.:

.. code-block:: python

    j = Job()
    j.application.exe = File('my_script2.sh')
    j.inputfiles = [ LocalFile('my_input.txt') ]
    j.outputfiles = [ LocalFile('my_output.txt') ]
    j.backend = Dirac()
    j.submit()


Input Data Usage
----------------

Generally, input data for a job is quite experiment specific. However, Ganga does provide some basic input data
functionality in Core that can be used to process a set of remotely stored files without copying them to the worker.
This is done with the ``GangaDataset`` object that takes a list of ``GangaFiles`` (as you would supply to the
``inputfiles`` field) and instead of copying them, a flat text file is created on the worker
(``__GangaInputData.txt__``) that lists the paths of the given input data. This is useful to access files from
Mass or Shared Storage using the mechanisms within the running program, e.g. Opening them with directly with Root.

As an example:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA INPUTDATA START
    :end-before: # -- INPUTANDOUTPUTDATA INPUTDATA STOP
    :dedent: 8


File Types Available
--------------------

Ganga provides several File types for accessing data from various sources. To find out what's available, do:

.. literalinclude:: ../../python/Ganga/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA GANGAFILES START
    :end-before: # -- INPUTANDOUTPUTDATA GANGAFILES STOP
    :dedent: 8


LocalFile
^^^^^^^^^

This is a basic file type that refers to a file on the submission host that Ganga runs on. As in input file,
it will pick up the file and send it with you job, as an output file it will be returned with your job and put in
the ``j.outputdir`` directory.

DiracFile
^^^^^^^^^

This will store/retrieve files from Dirac data storage. This will require a bit of configuration in ``~/.gangarc``
to set the correct LFN paths and also where you want the data to go:

.. code-block:: python

    config.DIRAC.DiracLFNBase
    config.DIRAC.DiracOutputDataSE


To use a DiracFile, do something similar to:

.. code-block:: python

    j = Job()
    j.application.exe = File('my_script2.sh')
    j.inputfiles = [ LocalFile('my_input.txt') ]
    j.outputfiles = [ DiracFile('my_output.txt') ]
    j.backend = Dirac()
    j.submit()


Ganga won't retrieve the output to the submission node so you need to do this manually:

.. code-block:: python

    j.outputfiles.get()

LCGSEFile
^^^^^^^^^

MassStorageFile
^^^^^^^^^^^^^^^

GoogleFile
^^^^^^^^^^

WebDAVFile
^^^^^^^^^^

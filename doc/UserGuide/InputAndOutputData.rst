Input And Output Data
=====================

Ganga tries to simplify sending input files and getting output files back as much as possible. You can specify
not only what files you want but where they should be retrieved/put. There are three fields that are relevant
for your job:

1. Input Files
    Files that are sent with the job and are available in the same directory on the worker node that runs it
2. Input Data
    A dataset or list of files that the job will run over but which are NOT transferred to the worker. Typically the running job will stream this data.
3. Output Files
    The name, type and location of the job output

Basic Input/Output File usage
-----------------------------

To start with, we'll show a job that sends an input text file with a job and then sends an output text file back:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA BASIC START
    :end-before: # -- INPUTANDOUTPUTDATA BASIC STOP
    :dedent: 8


After the job completes, you can then view the output directory and see the output file:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA PEEKOUTPUT START
    :end-before: # -- INPUTANDOUTPUTDATA PEEKOUTPUT STOP
    :dedent: 8


If the job doesn't produce the output Ganga was expecting, it will mark the job as failed:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA FAILJOB START
    :end-before: # -- INPUTANDOUTPUTDATA FAILJOB STOP
    :dedent: 8


You can also use wildcards in the files as well:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA WILDCARD START
    :end-before: # -- INPUTANDOUTPUTDATA WILDCARD STOP
    :dedent: 8


After completion, the output files found are copied as above but they are also recorded in the job appropriately:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
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

Generally, input data for a job is quite experiment specific. However, Ganga provides by default some basic input data functionality that can be used to process a set of remotely stored files without copying them to the worker.
This is done with the ``GangaDataset`` object that takes a list of ``GangaFiles`` (as you would supply to the
``inputfiles`` field) and instead of copying them, a flat text file is created on the worker
(``__GangaInputData.txt__``) that lists the paths of the given input data. This is useful to access files from
Mass or Shared Storage using the mechanisms within the running program, e.g. opening them with directly with Root.

As an example:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA INPUTDATA START
    :end-before: # -- INPUTANDOUTPUTDATA INPUTDATA STOP
    :dedent: 8


File Types Available
--------------------

Ganga provides several File types for accessing data from various sources. To find out what's available, do:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- INPUTANDOUTPUTDATA GANGAFILES START
    :end-before: # -- INPUTANDOUTPUTDATA GANGAFILES STOP
    :dedent: 8


LocalFile
^^^^^^^^^

This is a basic file type that refers to a file on the submission host that Ganga runs on. As an input file,
it will pick up the file and send it with your job, as an output file it will be returned with your job and put in
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


Ganga won't retrieve the output to the submission node so if you need it locally, you will have to do.

.. code-block:: python

    j.outputfiles.get()

Often it might be better to simply stream the data from its remote destination. You can get th ``URL`` for this as

.. code-block:: python

    j.outputfiles[0].accessURL()

    
GoogleFile
^^^^^^^^^^

This will store files to the user's Google Drive. This requires the user to authenticate and give restricted access to Google Drive.
To use a GangaFile, do something similar to:

.. code-block:: python

    j = GangaFile("mydata.txt")
    j.localDir = "~/temp"
    j.put()
    print(j)
    GoogleFile (
        namePattern = mydata.txt,
        localDir = /home/dumbmachine/temp,
        failureReason = ,
        compressed = False,
        downloadURL = https://drive.google.com/file/d/1dS_XqANroclWAqgIvLU7q5rbzen17mSf
    )

The urls are generated by using the `id` of the file.

This will upload the local file "~/temp/mydata.txt" to the user's Google Drive inside a folder names `Ganga`. The File object also supports for glob patterns, which can be supplied as `j.namePattern = '*.ROOT'`.

Upon first usage, the user will be asked to authenticate and allow access to create new files and edit these files only. While the default client ID of `Ganga` can be used, it is recommended to create you own client ID. Tjhis will prevent getting rate limited by other users. See :doc:`GoogleOauth` for how to do this.

Only files created by Ganga can be deleted (or restored after deletion).

.. code-block:: python

    j = GangaFile("mydata.txt")
    j.localDir = "~/temp"
    j.put()

    # if the file is required to be deleted
    j.remove() # will send the file to trash, use permanent=True for deletion
    # to restore the file from trash
    j.restore()

To download files previously uploaded by ganga, use the `get` method:

.. code-block:: python

    # consider "mydata.txt" file was previously uploaded by ganga
    j = GangaFile("mydata.txt")
    j.localDir = "~/temp" # folder where the file should be downloaded
    j.get()

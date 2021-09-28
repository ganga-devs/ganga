PostProcessors
==============

Ganga can be instructed to do many things after a job completes. Each object can be added to the ``postprocessors``
field of the ``Job`` object and they will be carried out in order. The available Post-Processing options are
detailed below:

Try it out
----------
When using the prime factorisation example from the Tutorial plugin (:doc:`TutorialPlugin`) it was not satisfactory that the individual prime factors were distributed over different files. A simple ``TextMerger`` can collate the numbers into a single file.

.. code-block:: python

    j = Job(application = PrimeFactorizer(number=268709474635016474894472456), \
            inputdata = PrimeTableDataset(table_id_lower=1, table_id_upper=30), \
            splitter = PrimeFactorizerSplitter(numsubjobs=10), \
	    postprocessors = TextMerger(files=['factors.dat']))

When the job has finished, there will now be a single file that we can look at

.. code-block:: python

    j.peek('factors.dat')

See below for how a ``CustomMerger`` could be used to provide a more unified output.

Mergers
-------

A merger is an object which will merge files from each subjobs and place it the master job output folder. The
method to merge depends on the type of merger object (or file type). For example, if each subjob produces a root
file 'thesis_data.root' and you would like this to be merged you can attach a RootMerger object to your job:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS APPEND START
    :end-before: # -- POSTPROCESSORS APPEND STOP
    :dedent: 8


When the job is finished this merger object will then merge the root files and place them in ``j.outputdir``. The
``ignorefailed`` flag toggles whether the merge can proceed if a subjob has failed. The overwrite flag toggles whether
to overwrite the output if it already exists. If a merger fails to merge, then the merger will fail the job and
subsequent postprocessors will not run. Also, be aware that the merger will only run if the files are available
locally, Ganga won't automatically download them for you (unless you use Tasks) to avoid running out of local space.
You can always run the mergers separately though:

.. code-block:: python

    j.postprocessors[0].merge()


There are several mergers available:

TextMerger
^^^^^^^^^^

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS TEXTMERGER START
    :end-before: # -- POSTPROCESSORS TEXTMERGER STOP
    :dedent: 8


Used for merging ``.txt``, ``.log``, etc. In addition to the normal attributes, you can also choose to compress
the output with

RootMerger
^^^^^^^^^^

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS TEXTMERGER START
    :end-before: # -- POSTPROCESSORS TEXTMERGER STOP
    :dedent: 8


Used for root files. In addition to the normal attributes, you can also pass additional arguments to hadd.

CustomMerger
^^^^^^^^^^^^

A custom merger where you can define your own merge function. For this merger to work you must supply a path to a
python module which carries out the merge with

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS CUSTOMMERGER START
    :end-before: # -- POSTPROCESSORS CUSTOMMERGER STOP
    :dedent: 8


In ``mymerger.py`` you must define a function called mergefiles(file_list,output_file), e.g:

.. code-block:: python

    import os
    def mergefiles(file_list,output_file):
          f_out = file(output_file,'w')
          for f in file_list:
                f_in = file(f)
                f_out.write(f_in.read())
                f_in.close()
          f_out.flush()
          f_out.close()


This function would mimic the TextMerger, but with more control to the user. Note that the ``overwrite`` and
``ignorefailed`` flags will still work here as a normal merger object.

SmartMerger
^^^^^^^^^^^

The final merger object which can be used is the ``SmartMerger()``, which will choose a merger object based on the
output file extension. It supports different file types. For example the following SmartMerger would use a RootMerger
for 'thesis_data.root' and a TextMerger for 'stdout'.

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS SMARTMERGER START
    :end-before: # -- POSTPROCESSORS SMARTMERGER STOP
    :dedent: 8


Note that:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS SMARTMERGERAPPEND START
    :end-before: # -- POSTPROCESSORS SMARTMERGERAPPEND STOP
    :dedent: 8


is equivalent to doing:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS SMARTMERGERAPPEND2 START
    :end-before: # -- POSTPROCESSORS SMARTMERGERAPPEND2 STOP
    :dedent: 8


However in the second instance you gain more control as you have access to the ``Root/TextMerger`` specific attributes,
but at the expense of more code. Choose which objects work best for you.


Checkers
--------

A checker is an object which will fail otherwise completed jobs based on certain conditions. However, if a checker is
misconfigured the default is to do nothing (pass the job), this is different to the merger.  Currently there are
three Checkers:

FileChecker
^^^^^^^^^^^

Checks the list of output files and fails job if a particular string is found (or not found). For example, you could do:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS FILECHECKER START
    :end-before: # -- POSTPROCESSORS FILECHECKER STOP
    :dedent: 8


You can also enforce that your file must exist, by setting ``filesMustExists`` to ``True``:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS FILECHECKERMUSTEXIST START
    :end-before: # -- POSTPROCESSORS FILECHECKERMUSTEXIST STOP
    :dedent: 8


If a job does not produce a stdout file, the checker will fail the job. This FileChecker will look in your stdout file
and grep the file for the string 'Segmentation'. If it finds it, the job will fail. If you want to fail the job
a string doesn’t exist, then you can do:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS FILECHECKEROPTS START
    :end-before: # -- POSTPROCESSORS FILECHECKEROPTS STOP
    :dedent: 8


In this case the FileChecker will fail the job if the string 'SUCCESS' is not found.


RootFileChecker
^^^^^^^^^^^^^^^

This checks that all your ROOT files are closed properly and have nonzero size. Also checks the merging procedure
worked properly. Adding a RootFileChecker to your job will add some protection against hadd failures, and ensure that
your ROOT files are mergable. If you do:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS ROOTFILECHECKER START
    :end-before: # -- POSTPROCESSORS ROOTFILECHECKER STOP
    :dedent: 8


This checker will check that each ROOT file has non-zero file size and is not a zombie. If you also have a merger,
it will check the output from hadd, ensure that the sum of the subjob entries is the same as the master job entries,
and check that each ROOT file has the same file structure. ``RootFileChecker`` inherits from ``FileChecker`` so you
can also ensure that the ROOT files must exist.

CustomChecker
^^^^^^^^^^^^^

This is probably the most useful checker and allows the user to use private python code to decide if a job should
fail or not. The ``CustomChecker`` will execute your script and fail the job based on the output. For example, you
can make a checker in your home directory called ``mychecker.py``. In this file you must define a function called
``check(j)``, which takes in your job as input and returns ``True`` (pass) or ``False`` (fail)

.. code-block:: python

    import os

    def check(j):
        outputfile = os.path.join(j.outputdir,'thesis_data.root')
        return os.path.exists(outputfile)


Then in ganga do:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS CUSTOMCHECKER START
    :end-before: # -- POSTPROCESSORS CUSTOMCHECKER STOP
    :dedent: 8


This checker will then fail jobs which don’t produce a 'thesis_data.root' root file.

Notifier
--------

The notifier is an object which will email you about your jobs upon completion. The default behaviour is to email
when master jobs have finished and when subjobs have failed. Emails are not sent upon failure if the auto-resubmit
feature is used. Important note: Emails will only be sent when ganga is running, and so the Notifier is only useful
if you have ganga running in the background (e.g. screen session, ``GangaService``). To make a notifier, just do
something like:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS NOTIFIER START
    :end-before: # -- POSTPROCESSORS NOTIFIER STOP
    :dedent: 8


If you want emails about every subjob, do

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS NOTIFIER START
    :end-before: # -- POSTPROCESSORS NOTIFIER STOP
    :dedent: 8


Management of post processors with your job
-------------------------------------------

You can add multiple post processors to a Job and Ganga will order them to some degree. Mergers appear first, then
checkers, then finally the notifier. It will preserve the order within each class though (e.g. The ordering of the
#checkers is defined by the user). To add some postprocessors to your job, you can do something like

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS MULTIPLE START
    :end-before: # -- POSTPROCESSORS MULTIPLE STOP
    :dedent: 8

or:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- POSTPROCESSORS MULTIPLE2 START
    :end-before: # -- POSTPROCESSORS MULTIPLE2 STOP
    :dedent: 8


You can also remove postprocessors:

.. code-block:: python

    In [21]:j.postprocessors
    Out[21]: [SmartMerger (
     files = [] ,
     ignorefailed = False ,
     overwrite = False
     ), FileChecker (
     files = [] ,
     checkSubjobs = False ,
     searchStrings = [] ,
     failIfFound = True
     ), Notifier (
     verbose = False ,
     address = ''
     )]

    In [22]:j.postprocessors.remove(FileChecker())

    In [23]:j.postprocessors
    Out[23]: [SmartMerger (
     files = [] ,
     ignorefailed = False ,
     overwrite = False
     ), Notifier (
     verbose = False ,
     address = ''
     )]


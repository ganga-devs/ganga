Miscellaneous Functionality
===========================

Ganga provides quite a lot of additional functionality to help with job management. Below are the main ones:

Job Templates
-------------

If there is a version of a job that you use a lot, it can be beneficial to store this as a job template and then
you can easily retrieve and then only alter a few parameters of. To create a template you do exactly what you would
do for a normal job except you create a ``JobTemplate`` object instead of a ``Job`` object:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- MISCFUNCTIONALITY TEMPLATE1 START
    :end-before: # -- MISCFUNCTIONALITY TEMPLATE1 STOP
    :dedent: 8


To view the ``templates`` available, just do:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- MISCFUNCTIONALITY TEMPLATE2 START
    :end-before: # -- MISCFUNCTIONALITY TEMPLATE2 STOP
    :dedent: 8


You can then create a job from this template by doing:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- MISCFUNCTIONALITY TEMPLATE3 START
    :end-before: # -- MISCFUNCTIONALITY TEMPLATE3 STOP
    :dedent: 8

Job Trees
---------

As you submit more jobs of different types, it can become quite difficult to keep track of them. Ganga supports a
`directory tree` like structure for jobs so you can easily keep track of which jobs are associated with different
calibrations, analyses, etc. Jobs are stored by `id` and can be thought of as soft links to the main Ganga Job
Repository.

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- MISCFUNCTIONALITY JOBTREE START
    :end-before: # -- MISCFUNCTIONALITY JOBTREE STOP
    :dedent: 8


GangaBox
--------

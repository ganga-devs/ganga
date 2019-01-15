Configuration
=============

There are several ways that you can configure and control how Ganga behaves. There are 3 different ways to do this:

1. Edit the options in your ``~/.gangarc`` file
2. Supply command line options:  ``ganga -o[Logging]Ganga.Lib=DEBUG``
3. At runtime using the `config` variable:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- CONFIGURATION VIEWCHANGE START
    :end-before: # -- CONFIGURATION VIEWCHANGE STOP
    :dedent: 8


The config system also provides a set of ``default_`` options for each Ganga object which override what values
the object starts with on creation. e.g.

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- CONFIGURATION DEFAULTCHANGE START
    :end-before: # -- CONFIGURATION DEFAULTCHANGE STOP
    :dedent: 8

In addition to this, you can also supply a ``~/.ganga.py`` file that will be executed just as if you'd typed the
commands when Ganga starts up e.g. this will show all running jobs when you start Ganga if put into the
``~/.ganga.py`` file:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- CONFIGURATION STARTUPSCRIPT START
    :end-before: # -- CONFIGURATION STARTUPSCRIPT STOP
    :dedent: 8

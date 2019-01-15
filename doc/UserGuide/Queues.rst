Queues
======

Many tasks in Ganga can take a lot of time from job submission to output download. Several things are already handled
in the background by the Monitoring System, but you may have user tasks that you want to also push into the background
that can run in parallel. This is where ``queues`` can be used.

To start with, you can view the state of the background threads by just typing ``queues``:

.. code-block:: python

    Ganga In [38]: queues
    Ganga Out [38]:
                            Ganga user threads:                         |             Ganga monitoring threads:
                            ------------------                          |              ------------------------
    Name                       Command                    Timeout       | Name                       Command                      Timeout
    ----                       -------                    -------       | ----                       -------                      -------
    User_Worker_0            idle                       N/A             | Ganga_Worker_0           idle                         N/A
    User_Worker_1            idle                       N/A             | Ganga_Worker_1           idle                         N/A
    User_Worker_2            idle                       N/A             | Ganga_Worker_2           idle                         N/A

    Ganga user queue:
    ----------------
    []
    Ganga monitoring queue:
    ----------------------
    []

    [12:57:37]
    Ganga In [39]:


To add a function call to the queue such as a submit call, do the following:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- QUEUES EXAMPLE START
    :end-before: # -- QUEUES EXAMPLE STOP
    :dedent: 8


You can also supply your own functions as well as provide arguments to these functions:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- QUEUES FUNCTION START
    :end-before: # -- QUEUES FUNCTION STOP
    :dedent: 8


Queues can also be used to submit subjobs in parallel:

.. literalinclude:: ../../ganga/GangaCore/test/GPI/TutorialTests.py
    :start-after: # -- QUEUES SPLIT START
    :end-before: # -- QUEUES SPLIT STOP
    :dedent: 8



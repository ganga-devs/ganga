Tutorial Plugin
===============

Ganga has a tutorial plugin available that serves the purposes of

* Illustrating how specific applications can be used (:doc:`UsingDifferentApplications`)
* Using specific splitters to divide a computational task into many pieces (:doc:`UsingDifferentApplications`)
* Illustrating how you can write your own plugin package for Ganga that provides new capability.

Enable tutorial plugin
----------------------
In the `Binder` tutorial, the tutorial plugin is already enabled and you do not have to do anything. For any other Ganga session, you can simply start Ganga like

.. code-block:: bash

    ganga -o '[Configuration]RUNTIME_PATH=GangaTutorial'

or edit the `RUNTIME_PATH` line of your `~/.gangarc` configuration file. 


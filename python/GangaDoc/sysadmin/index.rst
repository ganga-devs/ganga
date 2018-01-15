Guide for System Administrators
===============================

This section of the manual is intended for system administrators or interested individuals to describe how to install and manage Ganga.

Installation
------------

Historically Ganga was installed via a custom ``ganga-install`` script which would fetch the latest version and its dependencies.
We have since migrated away from that and there are two primary ways to get access to Ganga, one of which is mostly of interest only to particle physicists.

pip
^^^

At its simplest it is possbile to install ganga using the standard Python ``pip`` tool with a simple

.. code-block:: bash

    pip install ganga

CVMFS
^^^^^

CVMFS is a read-only file system intended for distributing software originally developed for the CERN virtual machine infrastructure.

``/cvmfs/ganga.cern.ch/``

Site config
-----------

It's often the case that you want to specify default configuration settings for your users, perhaps on a group-by-group basis.
You can do this by placing ``.gangarc``-style INI files in a common directory on your system and pointing Ganga at it.
The order of precedence for a particular setting goes ``default`` → ``site config`` → ``user config`` → ``runtime setting`` with those later in the chain overriding those earlier.
The location that Ganga looks for the site config is controlled with an environment variable, ``GANGA_SITE_CONFIG_AREA``, which you could set in your users' default shell setup.

.. code-block:: bash

    GANGA_SITE_CONFIG_AREA=/some/physics/subgroup

Files in this directory should be named after the Ganga version that you want to affect.
They should start with the version number with the ``.`` replaced with ``-`` and can have any extension.
So if you have three config files:

.. code-block:: bash

    $ ls $GANGA_SITE_CONFIG_AREA
    6-0-44.ini  6-1-6.ini  6-1-10.ini

and the user is running Ganga 6.1.6 then ``6-0-44`` and ``6-1-6`` will be loaded and ``6-1-10`` will be ignored.

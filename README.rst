Ganga
=====

Ganga is a tool to compose, run and track compute jobs across a variety of backends and application types.

Installation
------------

Ganga can be installed using the standard Python tool ``pip`` with

.. code-block:: bash

    pip install ganga

Usage
-----

Ganga primarily runs as a command-line tool so can be started by just running

.. code-block:: bash

    ganga

Which will load an interactive IPython prompt at which you can type

.. code-block:: python

    j = Job()
    j.submit()
    j.peek('stdout')

to create a simple local job which runs an executable which simply prints some text to the stdout.

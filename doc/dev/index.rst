Guide for developers
====================

This document is intended to detail some of the inner workings of Ganga to both document what we have done as well as make it easier for new developers to get on-board quicker.

GangaObject
-----------

At the core of a lot of Ganga is :class:`~.GangaObject`.
This is a class which provides most of the core functionality of Ganga including persistency, typed attribute checking and simplified construction.

.. note::
    There is currently some work being done to replace the existing implementation if ``GangaObject`` with a simpler version.
    The user-facing interface should not change at all but more modern Python features will be used to simplify the code.
    This will also affect how schemas are defined but not how they are presented or persisted.

Schema
------

The schema of a ``GangaObject`` defines the set of attributes belonging to that class along with their allowed types, access control, persistency etc.
Each ``GangaObject`` must define a schema which consists of a schema version number and a dictionary of :class:`~.Item`\ s.
Schema items must define their name and a default value and can optionally define a lot more such as a list of possible types and documentation string.

Proxy objects
-------------

In order to provide a nice interface to users, Ganga provides a :term:`Ganga Public Interface` which fulfils two main purposes.
Firstly it is a reduced set of objects so that the user is not bombarded with implementation details such as :class:`~.Node`.
Secondly, all ``GangaObjects`` available through the GPI are wrapped in a runtime-generated class called a *proxy*.

These proxy classes exist for a number of reasons but primarily they are there for access control.
While a ``GangaObject`` can has as many functions and attributes as it likes,
only those attributes in the schema and those methods which are explicitly exported will be available to users of the proxy class.

When working on internal Ganga code, you shuold never have to deal with any proxy objects at all.
Proxies should be added to objects as they are passed to the GPI and should be removed as they are passed back.

Attributes on proxy objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Proxy classes and the object that they are proxying have a set number of attributes which should be present.

If an object inherits from ``GangaObject`` the class can have the property ``_proxyClass`` set which will point to the relevant :class:`~.GPIProxyObject` subclass. This is created on demand in the ``addProxy`` and ``GPIProxyObjectFactory`` methods.
The proxy class (which is a subclass of ``GPIProxyObject`` and created using :func:`~.GPIProxyClassFactory`) will have the attribute `_impl` set to be the relevant ``GangaObject`` subclass.

When an instance of a proxy class is created, the `_impl` attribute of the instance will point to the instance of the ``GangaObject`` that is being proxied.


Repository
----------

A repository is the physical storage of data on disk (usually persisted ``GangaObjects``) as well as library interface to it.

Registry
--------

A registry is an in-memory data-store which is backed by a repository.

Job monitoring
--------------

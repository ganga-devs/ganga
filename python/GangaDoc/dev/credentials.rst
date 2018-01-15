Credentials
===========

Introduction
------------

The new credentials system implemented in Ganga came about to solve the problem of people wanting to be able to manage multiple jobs in one Ganga session using different credentials.
This could be because they are a member of multiple VOs or that they want to run both production and user jobs at once.

The old system worked by having two global credentials which are built into Ganga,
an AFS token which is needed when running on ``lxplus`` and a Grid proxy which is needed to submit Grid jobs.
All Grid jobs relied on it being present and would stop monitoring jobs if it's not.
This was rigid and did not allow for some of the more complex situations which can arise.

The new system
--------------

In order to gain the flexibility to have multiple concurrent credentials as well as adding the ability to add new credentials and easily defer their acquisition,
a new system was developed. It comprises three main parts:

Credential Requirement Classes
    This is the user-facing part. It is used to define the credentials that a particular backend or file needs.
    For example, the requirement may be that it needs a grid proxy for the LHCb VO with the production role.
    These classes are intended to be very lightweight and are little more than a prettified dictionary with helper methods.

Credential Info Classes
    The interaction with the actual credential file on disk is done through these classes.
    They store and cache information about the credential and allow renewal and creation of new credentials.

Credential Store
    Basically a dictionary-like interface between the two classes above.
    If you give an ``ICredentialRequirement`` to the store then it will return you a matching ``ICredentialInfo``.
    If it can't find a match in the store it will try to wrap an existing file on disk.
    You can force the creation of a new credential too.

Usage
-----

The primary user of the credentials system is the job's backend (though file objects will often also need to use it).
Each backend has an attribute called ``credential_requirements`` which contains an ``ICredentialRequirement`` object.
By default this is ``None`` for most backends and ``VomsProxy()`` for LCG backends.
The backend will then pass the credential requirement down the chain (the variable is usually called ``cred_req``) until it needs to be used,
at which point it will ask the store for the real credential and then extract the necessary information from it
(usually it wants the location of the credential file on disk).

A method decorator called :func:`~.require_credential` is provided for use in any class which has a ``credential_requirements`` attribute.
It will access this attribute, search in the credential store for the appropriate match and raise an error if it is not found.
This allows any methods on a class (such as a backend's ``submit`` method) to label themselves as using a credential,
allowing the system to defer asking the user to create the credential until the time it is acually needed.
It is *possible* to use the credentials system without using ``require_credential``
but it provides a way of explicitly marking "this is the point at which we will ask the user to create the credential."

As well as storing a list of all the known credentials, the credential store also provides a list of "needed credentials."
These are "credential requirements" which have been requested by some subsystem (usually running in a background thread)
but which could not find a corresponding credential file.
For example if you have no grid proxy on disk and start Ganga and it starts trying to monitor a job,
it will add the credential requirement to ``needed_credentials``
which is analysed by the IPython prompt code to inform the user that there's some credential missing.
The credential store exports a :meth:`~.CredentialStore.renew` method to scan ``needed_credentials``
and renew or create any expired or missing credentials.

When a backend is trying to monitor a job, it should check that the ``credential_requirements`` object can give a valid credential.
If it can't then the code should add ``credential_requirements`` to ``needed_credentials`` and abort.
If a backend it trying to do bulk monitoring then it should group/split the jobs by their credentials.

Recommendations
---------------

Throughout the majority of Ganga code, only ``ICredentialRequirements`` objects should be passed and handled.
Passing to the credential store and subsequent extraction of information should be done at the lowest level possible.
It is recommended that you never pass around ``ICredentialInfo`` objects if it can be avoided and instead operate directly on the return value from the store e.g.:

.. code-block:: python

    cred_req = VomsProxy(vo='lhcb')
    credential_store[cred_req].location

User API
--------

The API that is exposed to the user is intentionally small.
All they should care about is specifying their requirements (if they differ from the defaults)
and renewing expired credentials.

A user will set the required credential on a job backend for example by doing something like:

.. code-block:: python

    b = LCG(credential_requirements=VomsProxy(vo='gridpp'))
    j = Job(backend=b)

There's no need for them to manually reference that ``VomsProxy`` object after that point,
the internals of the system will keep track of it for them.
If at some point the credential expires and the monitoring is still running
then the user will be prompted to run ``credential_store.renew()``
which will try to regenerate any missing or expired credentials.

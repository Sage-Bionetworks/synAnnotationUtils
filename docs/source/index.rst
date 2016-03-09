Python Utility Package Documentation
====================================

Overview
---------
This utility package provides additional functions for Synapse Python client.

Annotation dictionary are generally stored in two formats:

* .yaml file
* Synapse Table

Requirements
-------------
* synapse client version > 1.2
* python version: 2.7 and above

Connecting to Synapse
---------------------
To use this package, you'll need to
`register <https://www.synapse.org/#!RegisterAccount:0>`_
for an account. The Synapse website can authenticate using a Google account,
but you'll need to take the extra step of creating a Synapse password
to use the programmatic clients.
Once that's done, you'll be able to load the library, create a :py:class:`Synapse` object and login::

 import synapseclient
 syn = synapseclient.Synapse()
 syn.login('me@nowhere.com', 'secret')

.. automodule:: pythonSynapseUtils
   :members:

.. toctree::
   :maxdepth: 1
   
   install
   yamlDict
   tableDict
   help


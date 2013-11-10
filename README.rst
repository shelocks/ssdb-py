ssdb-py
=======
A Python client for ssdb.

Installation
------------

To install ssdb-py,simply

.. code-block:: bash

    $ sudo python setup.py install


Getting Started
---------------

.. code-block:: pycon

    import ssdb
    ssdb=ssdb.SSDB(host='127.0.0.1',port=8888)
    ssdb.set('foo','bar')
    r=ssdb.get('foo')

More use case can find in tests.
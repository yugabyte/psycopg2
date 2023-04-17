psycopg2 - Python-PostgreSQL Database Adapter
=============================================

Psycopg is the most popular PostgreSQL database adapter for the Python
programming language.  Its main features are the complete implementation of
the Python DB API 2.0 specification and the thread safety (several threads can
share the same connection).  It was designed for heavily multi-threaded
applications that create and destroy lots of cursors and make a large number
of concurrent "INSERT"s or "UPDATE"s.

Psycopg 2 is mostly implemented in C as a libpq wrapper, resulting in being
both efficient and secure.  It features client-side and server-side cursors,
asynchronous communication and notifications, "COPY TO/COPY FROM" support.
Many Python types are supported out-of-the-box and adapted to matching
PostgreSQL data types; adaptation can be extended and customized thanks to a
flexible objects adaptation system.

Psycopg 2 is both Unicode and Python 3 friendly.


Documentation
-------------

Documentation is included in the ``doc`` directory and is `available online`__.

.. __: https://www.psycopg.org/docs/

For any other resource (source code repository, bug tracker, mailing list)
please check the `project homepage`__.

.. __: https://psycopg.org/


Installation
------------

Building Psycopg requires a few prerequisites (a C compiler, some development
packages): please check the install_ and the faq_ documents in the ``doc`` dir
or online for the details.

If prerequisites are met, you can install psycopg like any other Python
package, using ``pip`` to download it from PyPI_::

    $ pip install psycopg2-yugabytedb

or using ``setup.py`` if you have downloaded the source package locally::

    $ python setup.py build
    $ sudo python setup.py install

Note - The YugabyteDB Psycopg2 requires Postgresql version 11 or above (preferrably 14)

.. _PyPI: https://pypi.org/project/psycopg2-yugabytedb/
.. _install: https://www.psycopg.org/docs/install.html#install-from-source
.. _faq: https://www.psycopg.org/docs/faq.html#faq-compile

:Linux/OSX: |gh-actions|
:Windows: |appveyor|

.. |gh-actions| image:: https://github.com/psycopg/psycopg2/actions/workflows/tests.yml/badge.svg
    :target: https://github.com/psycopg/psycopg2/actions/workflows/tests.yml
    :alt: Linux and OSX build status

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/psycopg/psycopg2?branch=master&svg=true
    :target: https://ci.appveyor.com/project/psycopg/psycopg2/branch/master
    :alt: Windows build status

YugabyteDB Psycopg2 Features
============================

Yugabyte Psycopg2 driver is a distributed python driver for YSQL built on the PostgreSQL psycopg2 driver. Although the upstream PostgreSQL psycopg2 driver works with YugabyteDB, the Yugabyte driver enhances YugabyteDB by eliminating the need for external load balancers.

* It is cluster-aware, which eliminates the need for an external load balancer.
* It is topology-aware, which is essential for geographically-distributed applications. The driver uses servers that are part of a set of geo-locations specified by topology keys.

Load balancing
--------------

The Yugabyte Psycopg2 driver has the following load balancing features:

* Uniform load balancing

In this mode, the driver makes the best effort to uniformly distribute the connections to each YugabyteDB server. For example, if a client application creates 100 connections to a YugabyteDB cluster consisting of 10 servers, then the driver creates 10 connections to each server. If the number of connections are not exactly divisible by the number of servers, then a few may have 1 less or 1 more connection than the others. This is the client view of the load, so the servers may not be well balanced if other client applications are not using the Yugabyte JDBC driver.

* Topology-aware load balancing

Because YugabyteDB clusters can have servers in different regions and availability zones, the YugabyteDB JDBC driver is topology-aware, and can be configured to create connections only on servers that are in specific regions and zones. This is useful for client applications that need to connect to the geographically nearest regions and availability zone for lower latency; the driver tries to uniformly load only those servers that belong to the specified regions and zone.
The Yugabyte Psycopg2 driver can be configured with pooling as well.

Usage
-----

Load balancing connection properties:

The following connection properties need to be added to enable load balancing:

* load_balance - enable cluster-aware load balancing by setting this property to True; disabled by default.
* topology_keys - provide comma-separated geo-location values to enable topology-aware load balancing. Geo-locations can be provided as cloud.region.zone.
* yb-servers-refresh-interval - The list of servers, to balance the connection load on, are refreshed periodically every 5 minutes by default. This time can be regulated by this property.

Pass new connection properties for load balancing in the connection URL or in the dictionary. To enable uniform load balancing across all servers, you set the load-balance property to True in the URL, as per the following example.

Connection String::

    conn = psycopg2.connect("dbname=database_name host=hostname port=port user=username  password=password load_balance=true")

Connection Dictionary::

    conn = psycopg2.connect(user = 'username', password='xxx', host = 'hostname', port = 'port', dbname = 'database_name', load_balance='True')

To specify topology keys, you set the topology_keys property to comma separated values, as per the following example.

Connection String::

    conn = psycopg2.connect("dbname=database_name host=hostname port=port user=username  password=password load_balance=true topology_keys=cloud1.region1.zone1,cloud2.region2.zone2")

Connection Dictionary::

    conn = psycopg2.connect(user = 'username', password='xxx', host = 'hostname', port = 'port', dbname = 'database_name', load_balance='True', topology_keys='cloud1.region1.zone1,cloud2.region2.zone2')

Multiple topologies can also be passed to the Topology Keys property, and each of them can also be given a preference value, as per the following example.::

    conn = psycopg2.connect("host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys=cloud1.region1.zone1:1,cloud2.region2.zone2:2")

The preference value (appended after :) is optional. So it is compatible with previous syntax of specifying cloud placements.

Preference value :1 means primary placement zone(s), value :2 means first fallback, value :3 means second fallback and so on.
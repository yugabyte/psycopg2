"""A Python driver for PostgreSQL

psycopg is a PostgreSQL_ database adapter for the Python_ programming
language. This is version 2, a complete rewrite of the original code to
provide new-style classes for connection and cursor objects and other sweet
candies. Like the original, psycopg 2 was written with the aim of being very
small and fast, and stable as a rock.

Homepage: https://psycopg.org/

.. _PostgreSQL: https://www.postgresql.org/
.. _Python: https://www.python.org/

:Groups:
  * `Connections creation`: connect
  * `Value objects constructors`: Binary, Date, DateFromTicks, Time,
    TimeFromTicks, Timestamp, TimestampFromTicks
"""
# psycopg/__init__.py - initialization of the psycopg module
#
# Copyright (C) 2003-2019 Federico Di Gregorio  <fog@debian.org>
# Copyright (C) 2020-2021 The Psycopg Team
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

# Import modules needed by _psycopg to allow tools like py2exe to do
# their work without bothering about the module dependencies.

# Note: the first internal import should be _psycopg, otherwise the real cause
# of a failed loading of the C module may get hidden, see
# https://archives.postgresql.org/psycopg/2011-02/msg00044.php

# Import the DBAPI-2.0 stuff into top-level module.

from typing import List
from psycopg2._psycopg import (                     # noqa
    BINARY, NUMBER, STRING, DATETIME, ROWID,

    Binary, Date, Time, Timestamp,
    DateFromTicks, TimeFromTicks, TimestampFromTicks,

    Error, Warning, DataError, DatabaseError, ProgrammingError, IntegrityError,
    InterfaceError, InternalError, NotSupportedError, OperationalError,

    _connect, apilevel, threadsafety, paramstyle,
    __version__, __libpq_version__,
    connection, cursor as _cur, ConnectionInfo as _conninfo
)

from psycopg2.policies import ClusterAwareLoadBalancer,TopologyAwareLoadBalancer
from psycopg2.loadbalanceproperties import LoadBalanceProperties
# Register default adapters.

from psycopg2 import extensions as _ext
_ext.register_adapter(tuple, _ext.SQL_IN)
_ext.register_adapter(type(None), _ext.NoneAdapter)

# Register the Decimal adapter here instead of in the C layer.
# This way a new class is registered for each sub-interpreter.
# See ticket #52
from decimal import Decimal                         # noqa
from psycopg2._psycopg import Decimal as Adapter    # noqa
_ext.register_adapter(Decimal, Adapter)
del Decimal, Adapter

import re
import psycopg2.extensions
import threading

def connect(dsn=None, connection_factory=None, cursor_factory=None, **kwargs):
    """
    Create a new database connection.

    The connection parameters can be specified as a string:

        conn = psycopg2.connect("dbname=test user=postgres password=secret")

    or using a set of keyword arguments:

        conn = psycopg2.connect(database="test", user="postgres", password="secret")

    Or as a mix of both. The basic connection parameters are:

    - *dbname*: the database name
    - *database*: the database name (only as keyword argument)
    - *user*: user name used to authenticate
    - *password*: password used to authenticate
    - *host*: database host address (defaults to UNIX socket if not provided)
    - *port*: connection port number (defaults to 5433 if not provided)

    Using the *connection_factory* parameter a different class or connections
    factory can be specified. It should be a callable object taking a dsn
    argument.

    Using the *cursor_factory* parameter, a new default cursor factory will be
    used by cursor().

    Using *async*=True an asynchronous connection will be created. *async_* is
    a valid alias (for Python versions where ``async`` is a keyword).

    Any other keyword parameter will be passed to the underlying client
    library: the list of supported parameters depends on the library version.

    """
    
    kwasync = {}
    if 'async' in kwargs:
        kwasync['async'] = kwargs.pop('async')
    if 'async_' in kwargs:
        kwasync['async_'] = kwargs.pop('async_')

    lbprops = LoadBalanceProperties(dsn, **kwargs)
    if lbprops.hasLoadBalance() :
        conn = getConnectionBalanced(lbprops, connection_factory, cursor_factory, **kwasync)
        if conn != None:
            return conn
        print('Failed to apply load balancing, Trying normal connection')
    
    dsn = lbprops.getStrippedDSN()
    kwargs = lbprops.getStrippedProperties()
    dsn = _ext.make_dsn(dsn, **kwargs)
    conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
    if cursor_factory is not None:
        conn.cursor_factory = cursor_factory
    return conn

def getConnectionBalanced(lbprops, connection_factory, cursor_factory=None, **kwasync):
    kwargs = lbprops.getStrippedProperties()
    loadbalancer = lbprops.getAppropriateLoadBalancer()
    dsn = lbprops.getStrippedDSN()
    unreachableHosts = loadbalancer.getUnreachableHosts()
    failedHosts = unreachableHosts
    chosenHost = loadbalancer.getLeastLoadedServer(failedHosts)
    dsn = _ext.make_dsn(dsn,**kwargs)
    needsRefresh = loadbalancer.needsRefresh()
    if chosenHost == '' or needsRefresh:
        controlConnection = _connect(dsn, connection_factory=connection_factory, **kwasync)
        if cursor_factory is not None:
            controlConnection.cursor_factory = cursor_factory
        if not loadbalancer.refresh(controlConnection):
            return None
        if chosenHost != '':
            dsnhost = controlConnection.info.host_addr
            loadbalancer.updateConnectionMap(dsnhost, 1)
            loadbalancer.updateConnectionMap(chosenHost, -1)
        
        try:
            controlConnection.close()
        except Exception as e:
            print("Could not close control connection:", str(e))

        # Getting chosenHost again after refresh for the latest least loaded server
        
        chosenHost = loadbalancer.getLeastLoadedServer(failedHosts)
    
    if chosenHost == '':
        return None
    
    while chosenHost != '':
        try :
            dsn = getDSNWithChosenHost(loadbalancer,dsn,chosenHost)
            newconn = _connect(dsn, connection_factory=connection_factory, **kwasync)
            if cursor_factory is not None:
                newconn.cursor_factory = cursor_factory
            if not loadbalancer.refresh(newconn):
                loadbalancer.updateConnectionMap(chosenHost, -1)
                failedHosts.append(chosenHost)
                loadbalancer.setForRefresh()
            else:
                better_node_available = loadbalancer.hasMorePreferredNodes(chosenHost)
                if better_node_available:
                    print('A higher level node is available')
                    loadbalancer.decrementHostToNumConnCount(chosenHost)
                    newconn.close()
                    loadbalancer.has_better_node = False
                    return getConnectionBalanced(lbprops, connection_factory, cursor_factory, **kwasync)
                return newconn
        except OperationalError:
            print('Couldn\'t connect to ', chosenHost, ' adding to failed list')
            failedHosts.append(chosenHost)
            loadbalancer.updateFailedHosts(chosenHost)
            try :
                newconn.close()
            except Exception:
                print('For cleanup purposes')
            chosenHost = loadbalancer.getLeastLoadedServer(failedHosts)
    
def getDSNWithChosenHost(loadbalancer, dsn, chosenHost):
    port = loadbalancer.getPort(chosenHost)
    """
    Special case for connection URI
    """
    if 'postgresql://' in dsn or 'postgres://' in dsn:
        if '@' in dsn :
            host_parameter = '@' + chosenHost + ':' + str(port) + '/'
            HostRegex = re.compile(r'@([^/]*)/')
            dsn = HostRegex.sub(host_parameter, dsn)
            return dsn
        else :
            host_parameter = '://' + chosenHost + ':' + str(port) + '/'
            HostRegex = re.compile(r'://([^/]*)/')
            dsn = HostRegex.sub(host_parameter, dsn)
            return dsn

    host_parameter = 'host=' + chosenHost + ' '
    port_parameter = 'port=' + str(port) + ' '
    HostRegex = re.compile(r'host( )*=( )*(\S)*( )?')
    PortRegex = re.compile(r'port( )*=( )*[0-9]*(None)*( )?')
    dsn = HostRegex.sub(host_parameter, dsn)
    dsn = PortRegex.sub(port_parameter, dsn)
    return dsn
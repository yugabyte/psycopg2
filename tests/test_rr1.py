import json
import os
import subprocess
import unittest
import time
from urllib.request import urlopen
from psycopg2.loadbalanceproperties import LoadBalanceProperties

import psycopg2

base_url = "host=127.0.0.1,127.0.0.4 port=5433 user=yugabyte dbname=yugabyte yb_servers_refresh_interval=1 "
yb_install_path = os.getenv('YB_PATH')
num_connections = 12
control_host = "127.0.0.1"

def create_rr_cluster():
    
    print("Destroying earlier YBDB cluster, if any ...")
    exec_cmd(f"{yb_install_path}/bin/yb-ctl", "Could not stop earlier YBDB cluster", 10, "destroy")
    
    print("Starting a YBDB cluster with rf=3 ...")
    exec_cmd(f"{yb_install_path}/bin/yb-ctl", "Could not start YBDB cluster", 30, "create", "--rf", "3", 
              "--placement_info", "cloud1.datacenter1.rack1,cloud1.datacenter2.rack1,cloud1.datacenter3.rack1", 
              "--tserver_flags", "placement_uuid=live,max_stale_read_bound_time_ms=60000000")

    print("Configuring the YBDB cluster ...")
    exec_cmd(f"{yb_install_path}/bin/yb-admin", "Could not configure YBDB cluster", 10, 
              "-master_addresses", "127.0.0.1:7100,127.0.0.2:7100,127.0.0.3:7100", 
              "modify_placement_info", "cloud1.datacenter1.rack1,cloud1.datacenter2.rack1,cloud1.datacenter3.rack1", "3", "live")

    for i in range(2, 5):
        print(f"Adding RR node to the YBDB cluster ...")
        exec_cmd(f"{yb_install_path}/bin/yb-ctl", "Could not add a RR node to the YBDB cluster", 15, 
                  "add_node", "--placement_info", f"cloud1.datacenter{i}.rack1", "--tserver_flags", "placement_uuid=rr")

    print("Configuring the RR YBDB cluster ...")
    exec_cmd(f"{yb_install_path}/bin/yb-admin", "Could not configure RR cluster", 10, 
              "-master_addresses", "127.0.0.1:7100,127.0.0.2:7100,127.0.0.3:7100", 
              "add_read_replica_placement_info", "cloud1.datacenter2.rack1,cloud1.datacenter3.rack1,cloud1.datacenter4.rack1", "3", "rr")

    print("Started the cluster!")

def main():

    print("Starting startRRExample1() ...")
    create_rr_cluster()
    try:
        new_conn = [None] * 12

        urls = [
            f"{base_url}load_balance=false",
            f"{base_url}load_balance=any",
            f"{base_url}load_balance=only-rr",
            f"{base_url}load_balance=only-primary",
            f"{base_url}load_balance=prefer-rr",
            f"{base_url}load_balance=prefer-primary",
        ]

        expected_results = [
            [12, 0, 0, 0, 0, 0],
            [2, 2, 2, 2, 2, 2],
            [0, 0, 0, 4, 4, 4],
            [4, 4, 4, 0, 0, 0],
            [0, 0, 0, 4, 4, 4],
            [4, 4, 4, 0, 0, 0]
        ]

        keys = [
            'false',
            'any',
            'only-rr',
            'only-primary',
            'prefer-rr',
            'prefer-primary'
        ]

        for url, key, expected in zip(urls, keys, expected_results):
            print(f"Using connection url: {url}")
            create_connections(url, key, expected)

        # Stopping RR servers
        for i in range(4, 7):
            print(f"Stopping RR server {i} ...")
            exec_cmd(f"{yb_install_path}/bin/yb-ctl", "Could not stop the YBDB server", 10, "stop_node", str(i))

        time.sleep(6)
        
        url = f"{base_url}load_balance=prefer-rr"
        print(f"Using connection url:\n    {url}")
        create_connections(url, 'prefer-rr', [4, 4, 4, 0, 0, 0])

        url = f"{base_url}load_balance=only-rr"
        print(f"Using connection url:\n    {url}")
        create_connections(url, 'only-rr', [])

        # Restarting nodes
        for i in [5, 4, 6]:
            print(f"Restarting RR node {i}...")
            exec_cmd(f"{yb_install_path}/bin/yb-ctl", f"Could not restart RR node {i}", 20, "start_node", str(i), "--placement_info", f"cloud1.datacenter{(i-4)}.rack1")

        time.sleep(6)

        url = f"{base_url}load_balance=any"
        print(f"Using connection url: {url}")
        create_connections(url, 'any', [2, 2, 2, 2, 2, 2])

        # Stopping primary servers
        for i in range(1, 4):
            print(f"Stopping primary server {i} ...")
            exec_cmd(f"{yb_install_path}/bin/yb-ctl", "Could not stop the YBDB server", 10, "stop_node", str(i))
        
        time.sleep(6)
        control_host = "127.0.0.4"

        url = f"{base_url}load_balance=prefer-primary"
        print(f"Using connection url:\n    {url}")
        create_connections(url, 'prefer-primary', [0, 0, 0, 4, 4, 4])

        url = f"{base_url}load_balance=only-primary"
        print(f"Using connection url:\n    {url}")
        create_connections(url, 'only-primary', [])

        print("Closing the application ...")
    finally:
        exec_cmd(f"{yb_install_path}/bin/yb-ctl", "Could not destroy the YBDB cluster", 10, "status")
    

def exec_cmd(command, error_message, timeout, *args):
    try:
        subprocess.run([command] + list(args), check=True, timeout=timeout)
    except subprocess.CalledProcessError:
        print(error_message)

def create_connections(url, key, counts):
    connections = []
    for i in range(num_connections):
        try:
            conn  = psycopg2.connect(url)
            connections.append(conn)
        except psycopg2.OperationalError as e:
            if len(counts) == 0:
                error_message = str(e)  # Convert the error object to string to get the message

                if "Could not find a server to connect to." in error_message:
                    print("Got expected exception")
                else:
                    print(f"Some other error occurred: {error_message}")
                    exit(1)
            else:
                print(f"Some error occurred: {e}")
                exit(1)
    
    print(f"Created {num_connections} connections")

    j = 1
    for count in counts:
        if count != -1 :
            host = '127.0.0.' + str(j)
            verifyOn(host, count)
        
        j = j + 1

    if key != 'false':
        verifyLocalConnections(counts, key, 1)
    
    for conn1 in connections:
        conn1.close()

    if key != 'false':
        verifyLocalConnections(counts, key, 0)

def verifyOn(server, expectedCount):
    count = 0
    url = "http://" + server + ":13000/rpcz"
    print(url)
    try:
        try:
            response = urlopen(url)
        except Exception as ax:
            if expectedCount != 0:
                print(f'{ex}')
                exit(1)
            else:
                return
        data_json = json.loads(response.read())
        for connection in data_json["connections"]:
            if connection["backend_type"] == "client backend":
                count = count + 1
        print(f"{server}:{count}")
        if not count == expectedCount:
            if server == control_host:
                if not count == expectedCount + 1:
                    raise Exception("host: " + server + " Expected Count: "+ str(expectedCount) + " actualCount: " + str(count))
            else:
                raise Exception("host: " + server + " Expected Count: "+ str(expectedCount) + " actualCount: " + str(count))
    except Exception as ex:
        print(f'{ex}')
        exit(1)

def verifyLocalConnections(counts, key, k):
    
    try:
        if key not in LoadBalanceProperties.CONNECTION_MANAGER_MAP:
            raise Exception("Error: Load Balancer instance not found in CONNECTION_MANAGER_MAP")
        
        load_balance_instance = LoadBalanceProperties.CONNECTION_MANAGER_MAP.get(key)
        expected_counts = load_balance_instance.hostToNumConnMap

        j = 1
        for count in counts:
            if count != -1 and count != 0:
                host = '127.0.0.' + str(j)
                if host in expected_counts:
                    if expected_counts.get(host) != count * k:
                        raise Exception("host: " + host + " expected Count in hostToNumConnMap: "+ str(count) + " actualCount: " + str(expected_counts.get(host)))
                else:
                    raise Exception("host: " + host + " not found in hostToNumConnMap")
            j = j + 1

    except Exception as ex:
        print(expected_counts)
        print(f'{ex}')
        exit(1)

if __name__ == "__main__":
    main()
import time

import psycopg2
from urllib.request import urlopen
import json
import os
from psycopg2 import ClusterAwareLoadBalancer as _lb
from psycopg2.loadbalanceproperties import LoadBalanceProperties

num_connections = 12
control_host = '127.0.0.1'
url1 = "host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys="
yb_path = ""

def create_cluster():
    global yb_path
    yb_path = os.getenv('YB_PATH')
    os.system(yb_path+'/bin/yb-ctl destroy')
    os.system(yb_path+'/bin/yb-ctl create --rf 3 --placement_info \"aws.us-west.us-west-2a,aws.us-west.us-west-2b,aws.us-west.us-west-2c\"')

def create_cluster_with_six_nodes():
    global yb_path
    yb_path = os.getenv('YB_PATH')
    os.system(yb_path+'/bin/yb-ctl destroy')
    os.system(yb_path+'/bin/yb-ctl create --rf 3 --placement_info \"aws.us-west.us-west-1a\"')
    os.system(yb_path + '/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"')
    os.system(yb_path + '/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2b\"')
    os.system(yb_path + '/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2c\"')


def create_connections(url, tkValue, counts):
    connections = []
    final_url = url + tkValue
    for i in range(num_connections):
        conn  = psycopg2.connect(final_url)
        connections.append(conn)
    
    print(f"Created {num_connections} connections")

    j = 1
    for count in counts:
        if count != -1 :
            host = '127.0.0.' + str(j)
            verifyOn(host, count)
        
        j = j + 1
    
    verifyLocalConnections(counts, tkValue, 1)

    for conn1 in connections:
        conn1.close()
    
    time.sleep(5)
    
    verifyLocalConnections(counts, tkValue, 0)

def verifyLocalConnections(counts, tkValue, k):

    key = "true&" + tkValue + "&False"
    
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

def verifyOn(server, expectedCount):
    count = 0
    url = "http://" + server + ":13000/rpcz"
    try:
        try:
            response = urlopen(url)
        except Exception as ex:
            if expectedCount != 0:
                print("error opening " + url)
                print(f'{ex}')
                exit(1)
            else:
                # Node is expected to be down
                return
        data_json = json.loads(response.read())
        for connection in data_json["connections"]:
            if connection["backend_type"] == "client backend":
                count = count + 1
        print(f"{server}:{count}")
        if not count == expectedCount:
            # Control connection close() can fail sometimes
            if server == control_host:
                if not count == (expectedCount + 1):
                    raise Exception("host: " + server + " Expected Count: "+ str(expectedCount) + " actualCount: " + str(count))
            else:
                raise Exception("host: " + server + " Expected Count: "+ str(expectedCount) + " actualCount: " + str(count))
    except Exception as ex:
        print(f'{ex}')
        exit(1)

def main():

    create_cluster()

    # All valid/available placement zones

    time.sleep(5)

    create_connections(url1, "aws.us-west.us-west-2a,aws.us-west.us-west-2c", [6, 0, 6])
    create_connections(url1, "aws.us-west.us-west-2a,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", [6, 6, 0])
    create_connections(url1, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", [12, 0, 0])
    create_connections(url1, "aws.us-west.*,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", [4, 4, 4])
    create_connections(url1, "aws.us-west.*:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", [4, 4, 4])
    #
    # Some Invalid Connections
    #    
    create_connections(url1, "BAD.BAD.BAD:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", [0, 12, 0])
    create_connections(url1, "aws.us-west.us-west-2a:1,BAD.BAD.BAD:2,aws.us-west.us-west-2c:3", [12, 0, 0])
    create_connections(url1, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,BAD.BAD.BAD:3", [12, 0, 0])
    create_connections(url1, "BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.us-west-2c:3", [0, 0, 12])
    create_connections(url1, "BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.*:3", [4, 4, 4])
    
    print("Test 1 Passed")

def check_node_down_behaviour():
    # Start RF=3 cluster with 6 nodes and with placements (127.0.0.1, 127.0.0.2, 127.0.0.3) -> us-west-1a,
    # and 127.0.0.4 -> us-east-2a, 127.0.0.5 -> us-east-2b and 127.0.0.6 -> us-east-2c
    create_cluster_with_six_nodes()
    control_host = '127.0.0.4'
    url = "host=127.0.0.4 port=5433 user=yugabyte dbname=yugabyte yb_servers_refresh_interval=10 load_balance=True topology_keys="
    create_connections(url, 'aws.us-west.us-west-1a', [4, 4, 4])

    os.system(yb_path+"/bin/yb-ctl stop_node 1")
    os.system(yb_path+"/bin/yb-ctl stop_node 2")
    os.system(yb_path+"/bin/yb-ctl stop_node 3")

    # time.sleep(15)

    create_connections(url, "aws.us-west.us-west-1a", [-1, -1, -1, 4, 4, 4])
    create_connections(url, "aws.us-west.*:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:2,aws.us-east.us-east-2c:3", [-1, -1, -1, 6, 6, 0])

    print("Test 2 Passed")

def check_node_up_behaviour():
    # Start RF=3 cluster with 6 nodes
    # Placements:
    # 127.0.0.1, 127.0.0.2, 127.0.0.3 -> us-west-1a,
    # 127.0.0.4 -> us-east-2a
    # 127.0.0.5 -> us-east-2b
    # 127.0.0.6 -> us-east-2c
    create_cluster_with_six_nodes()
    control_host = '127.0.0.4'
    url = "host=127.0.0.4,127.0.0.5 port=5433 user=yugabyte dbname=yugabyte yb_servers_refresh_interval=10 load_balance=True topology_keys="
    create_connections(url, "aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4",[4, 4, 4, 0, 0, 0])

    os.system(yb_path + "/bin/yb-ctl stop_node 1")
    os.system(yb_path + "/bin/yb-ctl stop_node 2")
    os.system(yb_path + "/bin/yb-ctl stop_node 3")
    os.system(yb_path + "/bin/yb-ctl stop_node 4")
    control_host = '127.0.0.5'

    create_connections(url, "aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4",[-1, -1, -1, -1, 12, 0])

    os.system(yb_path + "/bin/yb-ctl start_node 4 --placement_info \"aws.us-east.us-east-2a\"")
    control_host = '127.0.0.4'
    time.sleep(15)

    create_connections(url, "aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4",[-1, -1, -1, 12, 0, 0])

    os.system((yb_path + "/bin/yb-ctl start_node 1 --placement_info \"aws.us-west.us-west-1a\""))
    os.system((yb_path + "/bin/yb-ctl start_node 2 --placement_info \"aws.us-west.us-west-1a\""))

    time.sleep(15)

    create_connections(url, "aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4",[6, 6, -1, 0, 0, 0])

    print("Test 3 Passed")



if __name__ == "__main__":
    main()
    check_node_down_behaviour()
    check_node_up_behaviour()
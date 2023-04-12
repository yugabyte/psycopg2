import psycopg2
from urllib.request import urlopen
import json
import os

numConnections = 12
url1 = "host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys="
yb_path = ""
def createCluster():
    global yb_path
    yb_path = os.getenv('YB_PATH')
    os.system(yb_path+'/bin/yb-ctl destroy')
    os.system(yb_path+'/bin/yb-ctl create --rf 3 --placement_info \"aws.us-west.us-west-2a,aws.us-west.us-west-2b,aws.us-west.us-west-2c\"')

def createClusterWithSixNodes():
    global yb_path
    yb_path = os.getenv('YB_PATH')
    os.system(yb_path+'/bin/yb-ctl destroy')
    os.system(yb_path+'/bin/yb-ctl create --rf 3 --placement_info \"aws.us-west.us-west-1a\" --tserver_flags \"placement_uuid=live,max_stale_read_bound_time_ms=60000000\"')
    os.system(yb_path + '/bin/yb-ctl add_node --placement_info \"aws.us-west.us-east-2a\"')
    os.system(yb_path + '/bin/yb-ctl add_node --placement_info \"aws.us-west.us-east-2b\"')
    os.system(yb_path + '/bin/yb-ctl add_node --placement_info \"aws.us-west.us-east-2c\"')


def createConnections(url, tkValue, counts):
    connections = []
    final_url = url + tkValue
    for i in range(numConnections):
        conn  = psycopg2.connect(final_url)
        connections.append(conn)
    
    print(f"Created {numConnections} connections")

    j = 1
    for count in counts:
        if count != -1 :
            host = '127.0.0.' + str(j)
            verifyOn(host, count)
        
        j = j + 1

    for conn1 in connections:
        conn1.close()


def verifyOn(server, expectedCount):
    count = 0
    url = "http://" + server + ":13000/rpcz"
    response = urlopen(url)
    data_json = json.loads(response.read())
    for connection in data_json["connections"]:
        if connection["backend_type"] == "client backend":
            count = count + 1
    print(f"{server}:{count}")
    assert count == expectedCount

def main():

    createCluster()

    # All valid/available placement zones

    createConnections(url1,"aws.us-west.us-west-2a,aws.us-west.us-west-2c",[6,0,6])
    createConnections(url1,"aws.us-west.us-west-2a,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2",[6,6,0])
    createConnections(url1,"aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3",[12,0,0])
    createConnections(url1, "aws.us-west.*,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", [4, 4, 4])
    createConnections(url1, "aws.us-west.*:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", [4, 4, 4])

    # Some Invalid Connections

    createConnections(url1, "BAD.BAD.BAD:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", [0, 12, 0])
    createConnections(url1, "aws.us-west.us-west-2a:1,BAD.BAD.BAD:2,aws.us-west.us-west-2c:3", [12, 0, 0])
    createConnections(url1, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,BAD.BAD.BAD:3", [12, 0, 0])
    createConnections(url1, "BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.us-west-2c:3", [0, 0, 12])
    createConnections(url1, "BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.*:3", [4, 4, 4]) 
    
    print("Test Passed")

def checkNodeDownBehaviour():
    # Start RF=3 cluster with 6 nodes and with placements (127.0.0.1, 127.0.0.2, 127.0.0.3) -> us-west-1a,
    # and 127.0.0.4 -> us-east-2a, 127.0.0.5 -> us-east-2b and 127.0.0.6 -> us-east-2c
    createClusterWithSixNodes()
    url = "host=127.0.0.4 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys="
    createConnections(url,'aws.us-west.us-west-1a',[4,4,4])

    os.system(yb_path+"/bin/yb-ctl stop_node 1")
    os.system(yb_path+"/bin/yb-ctl stop_node 2")
    os.system(yb_path+"/bin/yb-ctl stop_node 3")

    createConnections(url,"aws.us-west.us-west-1a", [-1, -1, -1, 4, 4, 4])

if __name__ == "__main__":
    main()
    checkNodeDownBehaviour()
import psycopg2
from urllib.request import urlopen
import json

numConnections = 12
url1 = "host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys="

def createConnections(url, tkValue, cnt1, cnt2, cnt3):
    connections = []
    final_url = url + tkValue
    for i in range(numConnections):
        conn  = psycopg2.connect(final_url)
        connections.append(conn)
    
    print(f"Created {numConnections} connections")

    verifyOn('127.0.0.1', cnt1)
    verifyOn('127.0.0.2', cnt2)
    verifyOn('127.0.0.3', cnt3)

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

    # All valid/available placement zones

    createConnections(url1,"aws.us-west.us-west-2a,aws.us-west.us-west-2c",6,0,6)
    createConnections(url1,"aws.us-west.us-west-2a,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2",6,6,0)
    createConnections(url1,"aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3",12,0,0)
    createConnections(url1, "aws.us-west.*,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", 4, 4, 4)
    createConnections(url1, "aws.us-west.*:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", 4, 4, 4)

    # Some Invalid Connections

    createConnections(url1, "BAD.BAD.BAD:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", 0, 12, 0)
    createConnections(url1, "aws.us-west.us-west-2a:1,BAD.BAD.BAD:2,aws.us-west.us-west-2c:3", 12, 0, 0)
    createConnections(url1, "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,BAD.BAD.BAD:3", 12, 0, 0)
    createConnections(url1, "BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.us-west-2c:3", 0, 0, 12)
    createConnections(url1, "BAD.BAD.BAD:1,BAD.BAD.BAD:2,aws.us-west.*:3", 4, 4, 4) 
    
    print("Test Passed")

if __name__ == "__main__":
    main()
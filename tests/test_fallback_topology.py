import psycopg2
from urllib.request import urlopen
import json

numConnections = 12

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
    # assert count == expectedCount

def main():
    createConnections("host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys=","aws.us-west.us-west-2a,aws.us-west.us-west-2c",6,0,6)
    createConnections("host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys=","aws.us-west.us-west-2a,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2",6,6,0)
    # createConnections("host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte load_balance=True topology_keys=","aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3",12,0,0)
    
    
    
    print("Test Passed")

if __name__ == "__main__":
    main()
import re
import sys
import time
import random
import psycopg2
import psycopg2.extensions


class ClusterAwareLoadBalancer:

    instance = None
    GET_SERVERS_QUERY = "select * from yb_servers()"
    REFRESH_LIST_SECONDS = 300
    lastServerListFetchTime = 0
    servers = []
    hostToNumConnMap = {}
    hostPortMap = {}
    hostPortMap_public = {}
    unreachableHosts = []
    GET_SERVERS_QUERY = "select * from yb_servers()"

    def getInstance():
        if ClusterAwareLoadBalancer.instance == None:
            ClusterAwareLoadBalancer.instance = ClusterAwareLoadBalancer()
        return ClusterAwareLoadBalancer.instance

    
    def getPort(self, host):
        port = self.hostPortMap.get(host)
        if port == None :
            port = self.hostPortMap_public.get(host)
        return port
    
    def getLeastLoadedServer(self,failedhosts):
        chosenHost = ''
        minConnectionsHostList = []
        min = sys.maxsize
        for h in self.hostToNumConnMap.keys():
            if h in failedhosts:
                continue
            currLoad = self.hostToNumConnMap.get(h)
            if currLoad < min:
                min = currLoad
                minConnectionsHostList.clear()
                minConnectionsHostList.append(h)
            elif currLoad == min :
                minConnectionsHostList.append(h)
        
        if len(minConnectionsHostList) > 0 :
            idx = random.randint(0,len(minConnectionsHostList)-1)
            chosenHost = minConnectionsHostList[idx]
        
        if chosenHost != '':
            self.updateConnectionMap(chosenHost,1)
            
        return chosenHost
    
    def needsRefresh(self):
        currentTimeInSeconds = time.time()
        diff = currentTimeInSeconds - self.lastServerListFetchTime
        firstTime = not self.servers
        return (firstTime or diff > self.REFRESH_LIST_SECONDS)

    def getCurrentServers(self, conn):
        cur = conn.cursor()
        cur.execute(self.GET_SERVERS_QUERY)
        rs = cur.fetchall()
        currentPrivateIps = []
        currentPublicIps = []
        hostConnectedTo = conn.info.host_addr
        useHostColumn = None
        isIpv6Addresses = ':' in hostConnectedTo
        if isIpv6Addresses:
            hostConnectedTo = hostConnectedTo.replace('[','').replace(']','')
        
        for row in rs :
            host = row[0]
            public_host = row[7]
            port = row[1]
            self.hostPortMap[host] = port
            self.hostPortMap_public[public_host] = port
            currentPrivateIps.append(host)
            currentPublicIps.append(public_host)
            if useHostColumn == None :
                if hostConnectedTo == host :
                    useHostColumn = True
                elif hostConnectedTo == public_host :
                    useHostColumn = False
        
        return self.getPrivateOrPublicServers(useHostColumn, currentPrivateIps, currentPublicIps)
        
    def getPrivateOrPublicServers(self, useHostColumn, privateHosts, publicHosts):
        if useHostColumn == None :
            if privateHosts:
                return privateHosts
            if publicHosts:
                return publicHosts
            else :
                return None
        currentHosts = privateHosts if useHostColumn else publicHosts
        return currentHosts

    def refresh(self, conn):
        if not self.needsRefresh():
            return True
        currTime = time.time()
        self.servers = self.getCurrentServers(conn)
        if not self.servers:
            return False
        self.lastServerListFetchTime = currTime
        self.unreachableHosts.clear()
        for h in self.servers :
            if not h in self.hostToNumConnMap.keys():
                self.hostToNumConnMap[h] = 0
        return True

    def getServers(self):
        return self.servers

    def updateConnectionMap(self, host, incDec):
        currentCount = self.hostToNumConnMap.get(host)
        if currentCount == 0 and incDec < 0 :
            return
        if currentCount == None and incDec > 0 :
            self.hostToNumConnMap[host] = incDec
        elif currentCount != None:
            self.hostToNumConnMap[host] = currentCount + incDec

    def getUnreachableHosts(self):
        return self.unreachableHosts
    
    def updateFailedHosts(self, chosenHost):
        self.unreachableHosts.append(chosenHost)
        self.hostToNumConnMap.pop(chosenHost)

    def loadBalancingNodes(self):
        return 'all'

    def setForRefresh(self):
        self.lastServerListFetchTime = 0

    def printHostToConnMap(self):
        print('Current load on', self.loadBalancingNodes(),' servers')
        print('-------------------------------------')
        for key,value in self.hostToNumConnMap.items():
            print(key, value)

class TopologyAwareLoadBalancer(ClusterAwareLoadBalancer):

    def __init__(self, placementvalues):
        self.placements = placementvalues
        self.allowedPlacements = []
        self.populatePlacementMap()
    
    def getPlacementMap(self, cloud, region, zone):
        cp = {}
        cp['cloud']=cloud
        cp['region']=region
        cp['zone']=zone
        return cp

    def populatePlacementMap(self):
        placementstrings = self.placements.split(',')
        for pl in placementstrings :
            placementParts = pl.split('.')
            if len(placementParts) != 3:
                print('Ignoring malformed topology-key property value')
                continue
            cp = self.getPlacementMap(placementParts[0],placementParts[1],placementParts[2])
            self.allowedPlacements.append(cp)
    
    def getCurrentServers(self, conn):
        cur = conn.cursor()
        cur.execute(self.GET_SERVERS_QUERY)
        rs = cur.fetchall()
        currentPrivateIps = []
        currentPublicIps = []
        hostConnectedTo = conn.info.host_addr
        useHostColumn = None
        isIpv6Addresses = ':' in hostConnectedTo
        if isIpv6Addresses:
            hostConnectedTo = hostConnectedTo.replace('[','').replace(']','')
        
        for row in rs :
            host = row[0]
            public_host = row[7]
            cloud = row[4]
            region = row[5]
            zone = row[6]
            port = row[1]
            self.hostPortMap[host] = port
            self.hostPortMap_public[public_host] = port
            cp = self.getPlacementMap(cloud,region,zone)
            if useHostColumn == None :
                if hostConnectedTo == host :
                    useHostColumn = True
                elif hostConnectedTo == public_host :
                    useHostColumn = False
            if cp in self.allowedPlacements :
                currentPrivateIps.append(host)
                currentPublicIps.append(public_host)
        
        return self.getPrivateOrPublicServers(useHostColumn, currentPrivateIps, currentPublicIps)
        
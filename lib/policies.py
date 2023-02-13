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
    DEFAULT_REFRESH_INTERVAL = 300
    refreshListSeconds = DEFAULT_REFRESH_INTERVAL


    def getInstance(refreshInterval):
        if ClusterAwareLoadBalancer.instance == None:
            ClusterAwareLoadBalancer.instance = ClusterAwareLoadBalancer()
            ClusterAwareLoadBalancer.instance.refreshListSeconds = refreshInterval if refreshInterval > 0 else 300 
        return ClusterAwareLoadBalancer.instance

    
    def getPort(self, host):
        port = self.hostPortMap.get(host)
        if port == None :
            port = self.hostPortMap_public.get(host)
        return port
    
    def getLeastLoadedServer(self,failedhosts):
        print(self.hostToNumConnMap)
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

    PRIMARY_PLACEMENTS = 1
    FIRST_FALLBACK = 2
    REST_OF_CLUSTER = -1
    MAX_PREFERENCE_VALUE = 10

    def __init__(self, placementvalues):
        self.hostToNumConnMap = {}
        self.hostPortMap = {}
        self.hostPortMap_public = {}
        self.placements = placementvalues
        self.allowedPlacements = {}
        self.fallbackPrivateIPs = {}
        self.fallbackPublicIPs = {}
        self.parseGeoLocations()

    def parseGeoLocations(self):
        values = self.placements.split(',')
        for value in values:
            v = value.split(':')
            if len(v) > 2 or value[-1] == ':':
               print('Invalid value part of property topology_keys:', value)
            if len(v) == 1:
                if self.PRIMARY_PLACEMENTS not in self.allowedPlacements:
                    primary = []
                    self.allowedPlacements[self.PRIMARY_PLACEMENTS] = primary
                else:
                    primary = self.allowedPlacements[self.PRIMARY_PLACEMENTS]
                self.populatePlacementSet(v[0],primary)
                self.allowedPlacements[self.PRIMARY_PLACEMENTS] = primary
            else:
                pref = int(v[1])
                if pref == 1:
                    primary= self.allowedPlacements.get(self.PRIMARY_PLACEMENTS)
                    if not primary:
                        primary = []
                        self.allowedPlacements[self.PRIMARY_PLACEMENTS] = primary
                    self.populatePlacementSet(v[0], primary)
                elif pref > 1 and pref <= self.MAX_PREFERENCE_VALUE :
                    fallbackPlacements = self.allowedPlacements.get(pref)
                    if not fallbackPlacements:
                        fallbackPlacements = []
                        self.allowedPlacements[pref] = fallbackPlacements
                    self.populatePlacementSet(v[0], fallbackPlacements)


    def populatePlacementSet(self,placements, allowedPlacements):
        pStrings = placements.split(',')
        for pl in pStrings:
            placementParts = pl.split('.')
            if len(placementParts) != 3 or placementParts[0] == '*' or placementParts[1] == '*':
                print('Ignoring malformed topology-key property value')
                continue
            cp = self.getPlacementMap(placementParts[0], placementParts[1], placementParts[2])

        allowedPlacements.append(cp)

    def getPlacementMap(self, cloud, region, zone):
        cp = {}
        cp['cloud']=cloud
        cp['region']=region
        cp['zone']=zone
        return cp

    
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
            self.updateCurrentHostList(currentPrivateIps, currentPublicIps, host, public_host, cloud, region, zone)
            if useHostColumn == None :
                if hostConnectedTo == host :
                    useHostColumn = True
                elif hostConnectedTo == public_host :
                    useHostColumn = False
        return self.getPrivateOrPublicServers(useHostColumn, currentPrivateIps, currentPublicIps)

    def getPrivateOrPublicServers(self, useHostColumn, privateHosts, publicHosts):
        servers = super().getPrivateOrPublicServers(useHostColumn, privateHosts, publicHosts)
        if servers:
            return servers
        for i in range(self.FIRST_FALLBACK,self.MAX_PREFERENCE_VALUE + 1):
            if self.fallbackPrivateIPs[i]:
                return super().getPrivateOrPublicServers(useHostColumn, self.fallbackPrivateIPs[i], self.fallbackPublicIPs[i])

        return super().getPrivateOrPublicServers(useHostColumn, self.fallbackPrivateIPs[self.REST_OF_CLUSTER], self.fallbackPublicIPs[self.REST_OF_CLUSTER])

    def checkIfPresent(self, cp, placements):
        for placement in placements:
            if cp == placement:
                return True
        
        return False

    def updateCurrentHostList(self, currentPrivateIps, currentPublicIps, host, public_host, cloud, region, zone):
        cp = self.getPlacementMap(cloud, region, zone)
        if self.checkIfPresent(cp, self.allowedPlacements[self.PRIMARY_PLACEMENTS]):
            currentPrivateIps.append(host)
            if len(public_host.strip()) != 0:
                currentPublicIps.append(public_host)
        else:
            print(cp)
            print(self.allowedPlacements)
            for key,value in self.allowedPlacements.items():
                if self.checkIfPresent(cp, value):
                    if key not in self.fallbackPrivateIPs:
                        hosts = []
                        self.fallbackPrivateIPs[key] = hosts
                    else:
                        hosts = self.fallbackPrivateIPs[key]
                    hosts.append(host)
                    self.fallbackPrivateIPs[key] = hosts
                    if len(public_host.strip()) != 0:
                        if key not in self.fallbackPublicIPs:
                            publicIPs = []
                            self.fallbackPublicIPs[key] = publicIPs
                        else:
                            publicIPs = self.fallbackPublicIPs[key]
                        publicIPs.append(public_host)
                        self.fallbackPublicIPs[key] = publicIPs
                    return

                if self.REST_OF_CLUSTER not in self.fallbackPrivateIPs:
                    remaininghosts = []
                    self.fallbackPrivateIPs[self.REST_OF_CLUSTER] = remaininghosts
                else:
                    remaininghosts = self.fallbackPrivateIPs[self.REST_OF_CLUSTER]
                remaininghosts.append(host)
                self.fallbackPrivateIPs[self.REST_OF_CLUSTER] = remaininghosts
                if len(public_host.strip()) != 0:
                    if self.REST_OF_CLUSTER not in self.fallbackPublicIPs:
                        remainingpublicIPs = []
                        self.fallbackPublicIPs[self.REST_OF_CLUSTER] = remainingpublicIPs
                    else:
                        remainingpublicIPs = self.fallbackPublicIPs[self.REST_OF_CLUSTER]
                    remainingpublicIPs.append(public_host)
                    self.fallbackPublicIPs[self.REST_OF_CLUSTER] = remainingpublicIPs



        
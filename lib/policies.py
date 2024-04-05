import re
import sys
import time
import random
import psycopg2
import psycopg2.extensions
import socket



class ClusterAwareLoadBalancer:

    instance = None
    GET_SERVERS_QUERY = "select * from yb_servers()"
    DEFAULT_FAILED_HOST_TTL_SECONDS = 5
    failedHostsTTL = DEFAULT_FAILED_HOST_TTL_SECONDS
    lastServerListFetchTime = 0
    servers = []
    hostToNumConnMap = {}
    hostToNumConnCount = {}
    hostPortMap = {}
    hostPortMap_public = {}
    hostToPriorityMap = {}
    unreachableHosts = {}
    currentPublicIps = []
    GET_SERVERS_QUERY = "select * from yb_servers()"
    DEFAULT_REFRESH_INTERVAL = 300
    refreshListSeconds = DEFAULT_REFRESH_INTERVAL
    useHostColumn = None
    def getInstance(refreshInterval, failedHostTTL):
        if ClusterAwareLoadBalancer.instance == None:
            ClusterAwareLoadBalancer.instance = ClusterAwareLoadBalancer()
            ClusterAwareLoadBalancer.instance.refreshListSeconds = refreshInterval if refreshInterval > 0 and refreshInterval < 600 else 300
            ClusterAwareLoadBalancer.instance.failedHostsTTL = failedHostTTL if failedHostTTL > 0 else ClusterAwareLoadBalancer.instance.DEFAULT_FAILED_HOST_TTL_SECONDS
        return ClusterAwareLoadBalancer.instance

    
    def getPort(self, host):
        port = self.hostPortMap.get(host)
        if port == None :
            port = self.hostPortMap_public.get(host)
        return port
    
    def getLeastLoadedServer(self,failedhosts):
        if not self.hostToNumConnMap and not self.currentPublicIps:
            privatehosts = []
            self.servers = self.getPrivateOrPublicServers(self.useHostColumn, privatehosts, self.currentPublicIps)
            if self.servers != None and self.servers.count != 0 :
                for h in self.servers:
                    if not h in self.hostToNumConnMap.keys():
                        if h in self.hostToNumConnCount.keys():
                            self.hostToNumConnMap[h] = self.hostToNumConnCount[h]
                        else:
                            self.hostToNumConnMap[h] = 0
            else:
                return '' 
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
        elif self.useHostColumn == None:
            newList = []
            newList = newList + self.currentPublicIps
            if newList:
                self.useHostColumn = False
                self.servers = newList
                self.unreachableHosts.clear()
                for h in self.servers:
                    if not h in self.hostToNumConnMap.keys():
                        if h in self.hostToNumConnCount.keys():
                            self.hostToNumConnMap[h] = self.hostToNumConnCount[h]
                        else:
                            self.hostToNumConnMap[h] = 0

                return self.getLeastLoadedServer(failedhosts)
            
        return chosenHost
    
    def needsRefresh(self):
        currentTimeInSeconds = time.time()
        diff = currentTimeInSeconds - self.lastServerListFetchTime
        firstTime = not self.servers
        return (firstTime or diff > self.refreshListSeconds)

    def getCurrentServers(self, conn):
        cur = conn.cursor()
        cur.execute(self.GET_SERVERS_QUERY)
        rs = cur.fetchall()
        currentPrivateIps = []
        hostConnectedTo = conn.info.host_addr
        isIpv6Addresses = ':' in hostConnectedTo
        if isIpv6Addresses:
            hostConnectedTo = hostConnectedTo.replace('[','').replace(']','')

        self.hostToPriorityMap.clear()

        for row in rs :
            host = row[0]
            public_host = row[7]
            host_addr = ''
            public_host_addr = ''
            try: 
                host_addr = socket.gethostbyname(host)
            except socket.gaierror as e:
                print(f'Error resolving {host}: {e}')
                raise e
            try:
                if public_host:
                    public_host_addr = socket.gethostbyname(public_host)
            except socket.gaierror as e:
                print(f'Error resolving {public_host}: {e}') 
                raise e
            cloud = row[4]
            region = row[5]
            zone = row[6]
            port = row[1]
            self.hostPortMap[host_addr] = port
            self.updatePriorityMap(host_addr,cloud,region,zone)
            self.hostPortMap_public[public_host_addr] = port
            if not host_addr in self.unreachableHosts.keys():
                currentPrivateIps.append(host_addr)
                self.currentPublicIps.append(public_host_addr)
            if self.useHostColumn == None :
                if hostConnectedTo == host_addr :
                    self.useHostColumn = True
                elif hostConnectedTo == public_host_addr :
                    self.useHostColumn = False
        
        return self.getPrivateOrPublicServers(self.useHostColumn, currentPrivateIps, self.currentPublicIps)
        
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
        self.lastServerListFetchTime = currTime
        possiblyReachableHosts = []
        for host, time_inactive in self.unreachableHosts.items():
            if (currTime - time_inactive) > self.failedHostsTTL:
                possiblyReachableHosts.append(host)

        emptyHostToNumConnMap = False
        for h in possiblyReachableHosts:
            self.unreachableHosts.pop(h, None)
            emptyHostToNumConnMap = True

        if emptyHostToNumConnMap and self.hostToNumConnMap:
            for h in self.hostToNumConnMap:
                self.hostToNumConnCount[h] = self.hostToNumConnMap[h]
            self.hostToNumConnMap.clear()
        self.servers = self.getCurrentServers(conn)
        if not self.servers:
            return False

        for h in self.servers :
            if not h in self.hostToNumConnMap.keys() and not h in self.unreachableHosts.keys():
                if h in self.hostToNumConnCount:
                    self.hostToNumConnMap[h] = self.hostToNumConnCount[h]
                else:
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

    def updatePriorityMap(self, host, cloud, region, zone):
        return
    def hasMorePreferredNodes(self, chosenHost):
        return False

    def decrementHostToNumConnCount(self, chosenHost):
        return
    def getUnreachableHosts(self):
        return list(self.unreachableHosts.keys())
    
    def updateFailedHosts(self, chosenHost):
        if not chosenHost in self.unreachableHosts:
            self.unreachableHosts[chosenHost] = time.time()
        self.hostToNumConnMap.pop(chosenHost, None)
        self.hostToNumConnCount.pop(chosenHost, None)

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

    def __init__(self, placementvalues, refreshInterval, failedHostTTL):
        self.hostToNumConnMap = {}
        self.hostToNumConnCount = {}
        self.hostToPriorityMap = {}
        self.hostPortMap = {}
        self.hostPortMap_public = {}
        self.currentPublicIps = {}
        self.placements = placementvalues
        self.allowedPlacements = {}
        self.fallbackPrivateIPs = {}
        self.fallbackPublicIPs = {}
        self.refreshListSeconds = refreshInterval if refreshInterval > 0 and refreshInterval < 600 else 300
        self.failedHostsTTL = failedHostTTL
        self.parseGeoLocations()

    def parseGeoLocations(self):
        values = self.placements.split(',')
        for value in values:
            v = value.split(':')
            if len(v) > 2 or value[-1] == ':':
               raise ValueError('Invalid value part of property topology_keys:', value)
            if len(v) == 1:
                primary = self.computeIfAbsent(self.allowedPlacements, self.PRIMARY_PLACEMENTS)
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


    def populatePlacementSet(self, placements, allowedPlacements):
        placementParts = placements.split('.')
        if len(placementParts) != 3 or placementParts[0] == '*' or placementParts[1] == '*':
            raise ValueError('Ignoring malformed topology-key property value')
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
        hostConnectedTo = conn.info.host_addr
        isIpv6Addresses = ':' in hostConnectedTo
        if isIpv6Addresses:
            hostConnectedTo = hostConnectedTo.replace('[','').replace(']','')
        self.hostToPriorityMap.clear()
        for row in rs :
            host = row[0]
            public_host = row[7]
            host_addr = ''
            public_host_addr = ''
            try: 
                host_addr = socket.gethostbyname(host)
            except socket.gaierror as e:
                print(f'Error resolving {host}: {e}')
                raise e
            try:
                if public_host:
                    public_host_addr = socket.gethostbyname(public_host)
            except socket.gaierror as e:
                print(f'Error resolving {public_host}: {e}')  
                raise e
            cloud = row[4]
            region = row[5]
            zone = row[6]
            port = row[1]
            self.hostPortMap[host_addr] = port
            self.updatePriorityMap(host_addr, cloud, region, zone)
            self.hostPortMap_public[public_host_addr] = port
            if not host_addr in self.unreachableHosts:
                self.updateCurrentHostList(currentPrivateIps, self.currentPublicIps, host_addr, public_host_addr, cloud, region, zone)
            if self.useHostColumn == None :
                if hostConnectedTo == host_addr :
                    self.useHostColumn = True
                elif hostConnectedTo == public_host_addr :
                    self.useHostColumn = False
        return self.getPrivateOrPublicServers(self.useHostColumn, currentPrivateIps, self.currentPublicIps)

    def hasMorePreferredNodes(self, chosenHost):
        if chosenHost in self.hostToPriorityMap:
            chosenHostPriority = self.hostToPriorityMap.get(chosenHost)
            if chosenHostPriority != None :
                for i in range(1,chosenHostPriority):
                    if i in self.hostToPriorityMap.values():
                        return True

        return False

    def updatePriorityMap(self, host, cloud, region, zone):
        if not host in self.unreachableHosts:
            priority = self.getPriority(cloud, region, zone)
            self.hostToPriorityMap[host] = priority

    def getPriority(self, cloud, region,zone):
        cp = self.getPlacementMap(cloud,region, zone)
        return self.getKeysByValue(cp)

    def getKeysByValue(self, cp):
        for i in range(1, self.MAX_PREFERENCE_VALUE + 1):
            if self.allowedPlacements.get(i):
                if self.checkIfPresent(cp, self.allowedPlacements.get(i)):
                    return i

        return self.MAX_PREFERENCE_VALUE + 1

    def updateFailedHosts(self, chosenHost):
        super().updateFailedHosts(chosenHost)
        if chosenHost in self.hostToPriorityMap.keys():
            self.hostToPriorityMap.pop(chosenHost)
        for i in range(self.FIRST_FALLBACK, self.MAX_PREFERENCE_VALUE + 1):
            if self.fallbackPrivateIPs.get(i):
                if chosenHost in self.fallbackPrivateIPs.get(i):
                    hosts = self.computeIfAbsent(self.fallbackPrivateIPs, i)
                    hosts.remove(chosenHost)
                    self.fallbackPrivateIPs[i] = hosts
                    return
            if self.fallbackPublicIPs.get(i):
                if chosenHost in self.fallbackPublicIPs.get(i):
                    hosts = self.computeIfAbsent(self.fallbackPublicIPs, i)
                    hosts.remove(chosenHost)
                    self.fallbackPublicIPs[i] = hosts
                    return
            if self.fallbackPrivateIPs.get(self.REST_OF_CLUSTER):
                if chosenHost in self.fallbackPrivateIPs.get(self.REST_OF_CLUSTER):
                    hosts = self.computeIfAbsent(self.fallbackPrivateIPs, self.REST_OF_CLUSTER)
                    hosts.remove(chosenHost)
                    self.fallbackPrivateIPs[self.REST_OF_CLUSTER] = hosts
                    return
            if self.fallbackPublicIPs.get(self.REST_OF_CLUSTER):
                if chosenHost in self.fallbackPublicIPs.get(self.REST_OF_CLUSTER):
                    hosts = self.computeIfAbsent(self.fallbackPublicIPs, self.REST_OF_CLUSTER)
                    hosts.remove(chosenHost)
                    self.fallbackPublicIPs[self.REST_OF_CLUSTER] = hosts
                    return

    def decrementHostToNumConnCount(self, chosenHost):
        currentCount = self.hostToNumConnCount.get(chosenHost)
        if currentCount != None and currentCount != 0 :
            self.hostToNumConnCount[chosenHost] = currentCount - 1

    def getPrivateOrPublicServers(self, useHostColumn, privateHosts, publicHosts):
        servers = super().getPrivateOrPublicServers(useHostColumn, privateHosts, publicHosts)
        if servers:
            return servers
        for i in range(self.FIRST_FALLBACK,self.MAX_PREFERENCE_VALUE + 1):
            if self.fallbackPrivateIPs.get(i):
                return super().getPrivateOrPublicServers(useHostColumn, self.fallbackPrivateIPs.get(i), self.fallbackPublicIPs.get(i))

        return super().getPrivateOrPublicServers(useHostColumn, self.fallbackPrivateIPs.get(self.REST_OF_CLUSTER), self.fallbackPublicIPs.get(self.REST_OF_CLUSTER))

    def checkIfPresent(self, cp, placements):
        for placement in placements:
            if placement['zone'] == '*':
                if placement['cloud'] == cp['cloud'] and placement['region'] == cp ['region']:
                    return True
        else :
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
            for key,value in self.allowedPlacements.items():
                if self.checkIfPresent(cp, value):
                    hosts = self.computeIfAbsent(self.fallbackPrivateIPs, key)
                    hosts.append(host)
                    self.fallbackPrivateIPs[key] = hosts
                    if len(public_host.strip()) != 0:
                        publicIPs = self.computeIfAbsent(self.fallbackPublicIPs, key)
                        publicIPs.append(public_host)
                        self.fallbackPublicIPs[key] = publicIPs
                    return
                
                remaininghosts = self.computeIfAbsent(self.fallbackPrivateIPs, self.REST_OF_CLUSTER)
                remaininghosts.append(host)
                self.fallbackPrivateIPs[self.REST_OF_CLUSTER] = remaininghosts

                if len(public_host.strip()) != 0:
                    remainingpublicIPs = self.computeIfAbsent(self.fallbackPublicIPs, self.REST_OF_CLUSTER)
                    remainingpublicIPs.append(public_host)
                    self.fallbackPublicIPs[self.REST_OF_CLUSTER] = remainingpublicIPs

    def computeIfAbsent(self, cloudplacement, index):
        if index not in cloudplacement:
            temp_set = []
            cloudplacement[index] = temp_set
        else:
            temp_set = cloudplacement[index]
        return temp_set



        
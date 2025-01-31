import re
import sys
import time
import random
import psycopg2
import psycopg2.extensions
import socket
import os
from psycopg2.logger import logger



class ClusterAwareLoadBalancer:

    unreachableHosts = {}
    hostToNumConnMap = {}
    useHostColumn = None
    lastServerListFetchTime = 0
    servers = {}
    primaryNodes = []
    rrNodes = []
    
    def __init__(self, loadBalance, refreshInterval, failedHostTTL):
        self.loadBalance = loadBalance
        self.failedHostsTTL = failedHostTTL if failedHostTTL >= 0 and failedHostTTL <= 60 else 5
        self.lastServerListFetchTime = 0
        self.servers = []
        self.primaryNodes = []
        self.rrNodes = []
        self.hostToNumConnMap = {}
        self.hostToNumConnCount = {}
        self.hostPortMap = {}
        self.hostPortMap_public = {}
        self.hostToPriorityMapPrimary = {}
        self.hostToPriorityMapRR = {}
        self.unreachableHosts = {}
        self.currentPrivateIps = {}
        self.currentPublicIps = {}
        self.refreshListSeconds = refreshInterval if refreshInterval >= 0 and refreshInterval <= 600 else 300
        self.useHostColumn = None
    
    def getPort(self, host):
        port = self.hostPortMap.get(host)
        if port == None :
            port = self.hostPortMap_public.get(host)
        return port

    def checkCurrentPublicIps(self):
        primaryhostlist = 0
        rrhostlist = 0
        if('primary' in self.currentPublicIps):
            primaryhostlist = len(self.currentPublicIps['primary'])
        if('read_replica' in self.currentPublicIps):
            rrhostlist = len(self.currentPublicIps['read_replica'])
        
        if primaryhostlist == 0 and rrhostlist == 0:
            return False
        if self.loadBalance == 'only-primary' and primaryhostlist == 0:
            return False
        if self.loadBalance == 'only-rr' and rrhostlist == 0:
            return False
                
        return True
    
    def getLeastLoadedServer(self,failedhosts):
        logger.debug("getLeastLoadedServer(): failed host list: %s, hostToNumConnMap: %s", failedhosts, self.hostToNumConnMap)
        if not self.hostToNumConnMap:
            privatehosts = {}
            if self.useHostColumn == True:
                publichosts = self.currentPublicIps
            else:
                publichosts = {}
            self.servers = self.getPrivateOrPublicServers(self.useHostColumn, privatehosts, publichosts)
            if self.servers:
                for h in self.servers:
                    if not h in self.hostToNumConnMap.keys():
                        if h in self.hostToNumConnCount.keys():
                            self.hostToNumConnMap[h] = self.hostToNumConnCount[h]
                        else:
                            self.hostToNumConnMap[h] = 0
            elif not self.checkCurrentPublicIps():
                return '' 
        chosenHost = ''
        minConnectionsHostList = []
        min = sys.maxsize
        for h in self.hostToNumConnMap.keys():
            if h in failedhosts:
                logger.debug("Skipping failed host " + h)
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
            newList = newList + self.getAppropriateHosts(self.currentPublicIps, False)
            if newList:
                logger.info("No host found, attempting the public ips...")
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
            
        logger.debug("Host chosen for new connection: " + chosenHost)
        return chosenHost
    
    def needsRefresh(self):
        currentTimeInSeconds = time.time()
        diff = currentTimeInSeconds - self.lastServerListFetchTime
        firstTime = not self.servers
        return (firstTime or diff > self.refreshListSeconds)

    # For Testing purpose only.
    def getRefreshQuery(self):
        test_yb_public_ip = os.getenv("TEST_YB_PUBLIC_IP")
        if test_yb_public_ip == "True":
            logger.warning("Refreshing cluster information from dummy data, to be only used for testing.")
            return "select * from test_yb_servers"
        else:
            logger.debug("Cluster info to be refreshed using yb_servers()")
            return "select * from yb_servers()"

    def getCurrentServers(self, conn):
        getServersQuery = self.getRefreshQuery()
        logger.debug("Executing query: " + getServersQuery + " to fetch list of servers")
        cur = conn.cursor()
        cur.execute(getServersQuery)
        rs = cur.fetchall()
        hostConnectedTo = conn.info.host_addr
        isIpv6Addresses = ':' in hostConnectedTo
        if isIpv6Addresses:
            hostConnectedTo = hostConnectedTo.replace('[','').replace(']','')

        self.hostToPriorityMapPrimary.clear()
        self.hostToPriorityMapRR.clear()
        self.currentPrivateIps.clear()
        self.currentPublicIps.clear()
        self.primaryNodes.clear()
        self.rrNodes.clear()

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
            node_type = row[3]
            cloud = row[4]
            region = row[5]
            zone = row[6]
            port = row[1]
            self.hostPortMap[host_addr] = port
            self.hostPortMap_public[public_host_addr] = port

            if self.useHostColumn == None :
                if hostConnectedTo == host_addr :
                    self.useHostColumn = True
                elif hostConnectedTo == public_host_addr :
                    self.useHostColumn = False
            
            logger.debug("getCurrentServers(): useHostColumn set as %s", self.useHostColumn)

            if self.useHostColumn == False:
                self.updatePriorityMap(public_host_addr,cloud,region,zone,node_type)
            else:
                self.updatePriorityMap(host_addr,cloud,region,zone,node_type)

            if self.useHostColumn != False and not host_addr in self.unreachableHosts.keys() or self.useHostColumn == False and not public_host in self.unreachableHosts.keys():
                if node_type == 'primary':
                    if self.useHostColumn == False:
                        self.primaryNodes.append(public_host)
                    else:
                        self.primaryNodes.append(host)
                else:
                    if self.useHostColumn == False:
                        self.rrNodes.append(public_host)
                    else:
                        self.rrNodes.append(host)
                self.populateHosts(self.currentPrivateIps, host_addr, node_type)
                if public_host_addr:
                    self.populateHosts(self.currentPublicIps, public_host_addr, node_type)
        
        return self.getPrivateOrPublicServers(self.useHostColumn, self.currentPrivateIps, self.currentPublicIps)
        
    def getPrivateOrPublicServers(self, useHostColumn, privateHosts, publicHosts):
        if useHostColumn == None :
            if privateHosts:
                return self.getAppropriateHosts(privateHosts, False)
            if publicHosts:
                return self.getAppropriateHosts(publicHosts, False)
            else :
                return None
        currentHosts = self.getAppropriateHosts(privateHosts, False) if useHostColumn else self.getAppropriateHosts(publicHosts, False)
        if not currentHosts:
            if (self.loadBalance == 'prefer-primary' and not self.primaryNodes) or (self.loadBalance == 'prefer-rr' and not self.rrNodes):
                currentHosts = self.getAppropriateHosts(privateHosts, True) if useHostColumn else self.getAppropriateHosts(publicHosts, True)
        logger.debug("List of servers got: %s, useHostColumn: %s", currentHosts, useHostColumn)
        return currentHosts

    def getAppropriateHosts(self, hosts, preferred):
        hostlist = []
        if not preferred:
            if self.loadBalance == 'any' or self.loadBalance == 'true':
                if('primary' in hosts):
                    hostlist = hostlist + hosts['primary']
                if('read_replica' in hosts):
                    hostlist = hostlist + hosts['read_replica']
                return hostlist
            elif self.loadBalance == 'only-rr' or self.loadBalance == 'prefer-rr':
                if('read_replica' in hosts):
                    hostlist = hostlist + hosts['read_replica']
                return hostlist
            elif self.loadBalance == 'only-primary' or self.loadBalance == 'prefer-primary':
                if('primary' in hosts):
                    hostlist = hostlist + hosts['primary']
                return hostlist
        else:
            if self.loadBalance == 'prefer-rr':
                hostlist = hostlist + self.primaryNodes
                return hostlist
            elif self.loadBalance == 'prefer-primary':
                hostlist = hostlist + self.rrNodes
                return hostlist


    def refresh(self, conn):
        if not self.needsRefresh():
            return True
        currTime = time.time()
        self.lastServerListFetchTime = currTime
        possiblyReachableHosts = []
        for host, time_inactive in self.unreachableHosts.items():
            if (currTime - time_inactive) > self.failedHostsTTL:
                logger.debug("Putting host  " + host + " into possiblyReachableHosts")
                possiblyReachableHosts.append(host)
            else:
                logger.debug("Not removing this host from unreachableHosts: " + host)

        emptyHostToNumConnMap = False
        for h in possiblyReachableHosts:
            self.unreachableHosts.pop(h, None)
            emptyHostToNumConnMap = True

        if emptyHostToNumConnMap and self.hostToNumConnMap:
            logger.debug("Clearing hostToNumConnMap: %s", self.hostToNumConnMap.keys())
            for h in self.hostToNumConnMap:
                self.hostToNumConnCount[h] = self.hostToNumConnMap[h]
            logger.debug("Hosts in hostToNumConnCount: %s", self.hostToNumConnCount)
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
                logger.debug("Added host " + h + " to hostToNumConnMap, with count %d", self.hostToNumConnMap[h])
        return True

    def getServers(self):
        return self.servers

    def updateConnectionMap(self, host, incDec):
        logger.debug("updating connection count for " + host + " by %d", incDec)
        currentCount = self.hostToNumConnMap.get(host)
        if currentCount == 0 and incDec < 0 :
            return
        if currentCount == None and incDec > 0 :
            self.hostToNumConnMap[host] = incDec
        elif currentCount != None:
            self.hostToNumConnMap[host] = currentCount + incDec

    def updatePriorityMap(self, host, cloud, region, zone, node_type):
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
        if chosenHost in self.primaryNodes:
            self.primaryNodes.remove(chosenHost)
        if chosenHost in self.rrNodes:
            self.rrNodes.remove(chosenHost)

    def loadBalancingNodes(self):
        return 'all'

    def setForRefresh(self):
        self.lastServerListFetchTime = 0

    def printHostToConnMap(self):
        print('Current load on', self.loadBalancingNodes(),' servers')
        print('-------------------------------------')
        for key,value in self.hostToNumConnMap.items():
            print(key, value)
    
    def populateHosts(self, hostMap, host, node_type):
        if node_type not in hostMap:
            temp_set = []
            temp_set.append(host)
            hostMap[node_type] = temp_set
        else:
            temp_set = hostMap[node_type]
            temp_set.append(host)
            hostMap[node_type] = temp_set

class TopologyAwareLoadBalancer(ClusterAwareLoadBalancer):

    PRIMARY_PLACEMENTS = 1
    FIRST_FALLBACK = 2
    REST_OF_CLUSTER = -1
    MAX_PREFERENCE_VALUE = 10

    def __init__(self, loadBalance, placementvalues, refreshInterval, failedHostTTL, explicitFallbackOnly):
        self.hostToNumConnMap = {}
        self.hostToNumConnCount = {}
        self.hostToPriorityMapPrimary = {}
        self.hostToPriorityMapRR = {}
        self.hostPortMap = {}
        self.hostPortMap_public = {}
        self.currentPrivateIps = {}
        self.currentPublicIps = {}
        self.placements = placementvalues
        self.loadBalance = loadBalance
        self.explicitFallbackOnly = explicitFallbackOnly
        self.allowedPlacements = {}
        self.fallbackPrivateIPs = {}
        self.fallbackPublicIPs = {}
        self.refreshListSeconds = refreshInterval if refreshInterval >= 0 and refreshInterval <= 600 else 300
        self.failedHostsTTL = failedHostTTL if failedHostTTL >= 0 and failedHostTTL <= 60 else 5
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
        logger.debug("Adding placement %s to allowed list", cp)
        allowedPlacements.append(cp)

    def getPlacementMap(self, cloud, region, zone):
        cp = {}
        cp['cloud']=cloud
        cp['region']=region
        cp['zone']=zone
        return cp

    
    def getCurrentServers(self, conn):
        getServersQuery = self.getRefreshQuery()
        logger.debug("Executing query: " + getServersQuery + " to fetch list of servers")
        cur = conn.cursor()
        cur.execute(getServersQuery)
        rs = cur.fetchall()
        hostConnectedTo = conn.info.host_addr
        isIpv6Addresses = ':' in hostConnectedTo
        if isIpv6Addresses:
            hostConnectedTo = hostConnectedTo.replace('[','').replace(']','')
        
        self.hostToPriorityMapPrimary.clear()
        self.hostToPriorityMapRR.clear()
        self.currentPrivateIps.clear()
        self.currentPublicIps.clear()
        self.fallbackPrivateIPs.clear()
        self.fallbackPublicIPs.clear()
        self.primaryNodes.clear()
        self.rrNodes.clear()

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
            node_type = row[3]
            region = row[5]
            zone = row[6]
            port = row[1]
            self.hostPortMap[host_addr] = port
            self.hostPortMap_public[public_host_addr] = port

            if self.useHostColumn == None :
                if hostConnectedTo == host_addr :
                    self.useHostColumn = True
                elif hostConnectedTo == public_host_addr :
                    self.useHostColumn = False
            
            logger.debug("getCurrentServers(): useHostColumn set as %s", self.useHostColumn)

            if self.useHostColumn == False:
                self.updatePriorityMap(public_host_addr,cloud,region,zone,node_type)
            else:
                self.updatePriorityMap(host_addr,cloud,region,zone,node_type)

            if self.useHostColumn != False and not host_addr in self.unreachableHosts.keys() or self.useHostColumn == False and not public_host in self.unreachableHosts.keys():
                if node_type == 'primary':
                    if self.useHostColumn == False:
                        self.primaryNodes.append(public_host)
                    else:
                        self.primaryNodes.append(host)
                else:
                    if self.useHostColumn == False:
                        self.rrNodes.append(public_host)
                    else:
                        self.rrNodes.append(host)
                self.updateCurrentHostList(self.currentPrivateIps, self.currentPublicIps, host_addr, public_host_addr, cloud, region, zone, node_type)
        return self.getPrivateOrPublicServers(self.useHostColumn, self.currentPrivateIps, self.currentPublicIps)

    def hasMorePreferredNodes(self, chosenHost):
        if self.loadBalance == 'only-rr' or self.loadBalance == 'prefer-rr' or self.loadBalance == 'any' or self.loadBalance == 'true':
            if chosenHost in self.hostToPriorityMapRR:
                chosenHostPriority = self.hostToPriorityMapRR.get(chosenHost)
                if chosenHostPriority != None :
                    for i in range(1,chosenHostPriority):
                        if i in self.hostToPriorityMapRR.values():
                            return True
        
        if self.loadBalance == 'only-primary' or self.loadBalance == 'prefer-primary' or self.loadBalance == 'any' or self.loadBalance == 'true':
            if chosenHost in self.hostToPriorityMapPrimary:
                chosenHostPriority = self.hostToPriorityMapPrimary.get(chosenHost)
                if chosenHostPriority != None :
                    for i in range(1,chosenHostPriority):
                        if i in self.hostToPriorityMapPrimary.values():
                            return True

        return False

    def updatePriorityMap(self, host, cloud, region, zone, node_type):
        if not host in self.unreachableHosts:
            priority = self.getPriority(cloud, region, zone)
            logger.debug("Priority of host %s = %d", host, priority)
            if node_type == 'primary':
                self.hostToPriorityMapPrimary[host] = priority
            else:
                self.hostToPriorityMapRR[host] = priority


    def getPriority(self, cloud, region,zone):
        cp = self.getPlacementMap(cloud,region, zone)
        return self.getKeysByValue(cp)

    def getKeysByValue(self, cp):
        for i in range(1, self.MAX_PREFERENCE_VALUE + 1):
            if self.allowedPlacements.get(i):
                if self.checkIfPresent(cp, self.allowedPlacements.get(i)):
                    logger.debug("Returning priority %d", i)
                    return i
        logger.debug("CloudPlacement %s does not belong to primary placement or any of the " +
            "fallback placements so returning %d as priority", cp, self.MAX_PREFERENCE_VALUE + 1)
        return self.MAX_PREFERENCE_VALUE + 1

    def updateFailedHosts(self, chosenHost):
        logger.debug("Marking %s as Down", chosenHost)
        super().updateFailedHosts(chosenHost)

        if chosenHost in self.hostToPriorityMapPrimary.keys():
            self.hostToPriorityMapPrimary.pop(chosenHost)
        if chosenHost in self.hostToPriorityMapRR.keys():
            self.hostToPriorityMapRR.pop(chosenHost)
        if chosenHost in self.primaryNodes:
            self.primaryNodes.remove(chosenHost)
        if chosenHost in self.rrNodes:
            self.rrNodes.remove(chosenHost)

        for i in range(self.FIRST_FALLBACK, self.MAX_PREFERENCE_VALUE + 1):
            if self.removeHost(self.fallbackPrivateIPs.get(i), chosenHost):
                logger.debug("Removing failed host %s from fallbackPrivateIPs level %d", chosenHost, (i - 1))
                return
            if self.removeHost(self.fallbackPublicIPs.get(i), chosenHost):
                logger.debug("Removing failed host %s from fallbackPublicIPs level %d", chosenHost, (i - 1))
                return
            
        if self.removeHost(self.fallbackPrivateIPs.get(self.REST_OF_CLUSTER), chosenHost):
            return
        
        if self.removeHost(self.fallbackPublicIPs.get(self.REST_OF_CLUSTER), chosenHost):
            return

        return

    def decrementHostToNumConnCount(self, chosenHost):
        logger.debug("Decreasing connection count of " + chosenHost)
        currentCount = self.hostToNumConnCount.get(chosenHost)
        if currentCount != None and currentCount != 0 :
            self.hostToNumConnCount[chosenHost] = currentCount - 1

    def getPrivateOrPublicServers(self, useHostColumn, privateHosts, publicHosts):
        servers = super().getPrivateOrPublicServers(useHostColumn, privateHosts, publicHosts)
        if servers:
            return servers
        for i in range(self.FIRST_FALLBACK,self.MAX_PREFERENCE_VALUE + 1):
            if useHostColumn != False and self.checkIfNodeTypePresent(self.fallbackPrivateIPs.get(i)) or useHostColumn == False and self.checkIfNodeTypePresent(self.fallbackPublicIPs.get(i)):
                logger.debug("Attempting to connect servers in fallback level-%d", (i - 1))
                servers = super().getPrivateOrPublicServers(useHostColumn, self.fallbackPrivateIPs.get(i), self.fallbackPublicIPs.get(i))
                return servers

        if self.explicitFallbackOnly and self.loadBalance != 'prefer-primary' and self.loadBalance != 'prefer-rr':
            return []
            
        if useHostColumn != False and self.checkIfNodeTypePresent(self.fallbackPrivateIPs.get(self.REST_OF_CLUSTER)) or useHostColumn == False and self.checkIfNodeTypePresent(self.fallbackPublicIPs.get(self.REST_OF_CLUSTER)):
            logger.debug("Returning servers from rest of the cluster.")
            servers = super().getPrivateOrPublicServers(useHostColumn, self.fallbackPrivateIPs.get(self.REST_OF_CLUSTER), self.fallbackPublicIPs.get(self.REST_OF_CLUSTER))
            return servers
        
        return [] 

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

    def updateCurrentHostList(self, currentPrivateIps, currentPublicIps, host, public_host, cloud, region, zone, node_type):
        cp = self.getPlacementMap(cloud, region, zone)
        if self.checkIfPresent(cp, self.allowedPlacements[self.PRIMARY_PLACEMENTS]):
            logger.debug("AllowedPlacements set: %s returned contains true for cp: %s" , self.allowedPlacements, cp)
            self.populateHosts(currentPrivateIps, host, node_type)
            if len(public_host.strip()) != 0:
                self.populateHosts(currentPublicIps, public_host, node_type)
        else:
            for key,value in self.allowedPlacements.items():
                if self.checkIfPresent(cp, value):
                    logger.debug("CloudPlacement %s is part of fallback levels", cp)
                    hosts = self.computeDictIfAbsent(self.fallbackPrivateIPs, key)
                    self.populateHosts(hosts, host, node_type)
                    self.fallbackPrivateIPs[key] = hosts
                    if len(public_host.strip()) != 0:
                        publicIPs = self.computeDictIfAbsent(self.fallbackPublicIPs, key)
                        self.populateHosts(publicIPs, public_host, node_type)
                        self.fallbackPublicIPs[key] = publicIPs
                    return
                
            remaininghosts = self.computeDictIfAbsent(self.fallbackPrivateIPs, self.REST_OF_CLUSTER)
            self.populateHosts(remaininghosts, host, node_type)
            self.fallbackPrivateIPs[self.REST_OF_CLUSTER] = remaininghosts

            if len(public_host.strip()) != 0:
                remainingpublicIPs = self.computeDictIfAbsent(self.fallbackPublicIPs, self.REST_OF_CLUSTER)
                self.populateHosts(remainingpublicIPs, public_host, node_type)
                self.fallbackPublicIPs[self.REST_OF_CLUSTER] = remainingpublicIPs
            
            logger.debug("AllowedPlacements set: %s returned contains false for cp: %s", self.allowedPlacements, cp)

    def computeDictIfAbsent(self, cloudplacement, index):
        if index not in cloudplacement:
            temp_set = {}
            cloudplacement[index] = temp_set
        else:
            temp_set = cloudplacement[index]
        return temp_set
    
    def computeIfAbsent(self, cloudplacement, index):
        if index not in cloudplacement:
            temp_set = []
            cloudplacement[index] = temp_set
        else:
            temp_set = cloudplacement[index]
        return temp_set
    
    def populateHosts(self, hostMap, host, node_type):
        if node_type not in hostMap:
            temp_set = []
            temp_set.append(host)
            hostMap[node_type] = temp_set
        else:
            temp_set = hostMap[node_type]
            temp_set.append(host)
            hostMap[node_type] = temp_set
    
    def removeHost(self, hostMap, host):
        if not hostMap:
            return False
        for key,values in hostMap.items():
            if host in values:
                values.remove(host)
                hostMap[key] = values
                return True
        return False
    
    def checkIfNodeTypePresent(self, hostMap):
        if not hostMap:
            return False
        if self.loadBalance == 'only-rr' or self.loadBalance == 'prefer-rr' or self.loadBalance == 'any' or self.loadBalance == 'true':
            if 'read_replica' in hostMap:
                temp_set = hostMap['read_replica']
                if len(temp_set) != 0:
                    return True
        
        if self.loadBalance == 'only-primary' or self.loadBalance == 'prefer-primary' or self.loadBalance == 'any' or self.loadBalance == 'true':
            if 'primary' in hostMap:
                temp_set = hostMap['primary']
                if len(temp_set) != 0:
                    return True
        
        return False
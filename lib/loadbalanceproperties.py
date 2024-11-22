import re
from psycopg2.policies import ClusterAwareLoadBalancer,TopologyAwareLoadBalancer
class LoadBalanceProperties:

    CONNECTION_MANAGER_MAP = {}
    placements = ''
    refreshInterval = 300
    failed_host_ttl_seconds = 5
    fallback_to_topology_keys_only = False

    def __init__(self, dsn, **kwargs):
        self.LOAD_BALANCE_PROPERTY_KEY = 'load_balance'
        self.EQUALS = '='
        self.originalDSN = dsn
        self.originalProperties = kwargs
        self.loadbalance = 'false'
        self.placements = ''
        self.key = ''
        self.refreshInterval = 300
        self.ybProperties = self.originalProperties
        self.ybDSN = None
        if self.originalDSN != None :
            self.ybDSN = self.processURL()
        else:
            self.ybProperties = self.processProperties()
        

    def processURL(self):
        ClusterAwareRegex = re.compile(r'load_balance( )*=( )*([a-z]*[A-Z]*)*-?([a-z]*[A-Z]*)*( )?')
        lb_string = ClusterAwareRegex.search(self.originalDSN)
        if lb_string == None :
            return self.originalDSN
        else:
            lb_string = lb_string.group()
            lb_parts = lb_string.split(self.EQUALS)
            lb_parts = list(filter(('').__ne__, lb_parts))
            propValue = lb_parts[1].lower().strip()
            if (propValue == 'true' or propValue == 'any' or propValue == 'only-rr' or propValue == 'only-primary' or propValue == 'prefer-rr' or propValue == 'prefer-primary' or propValue == 'false'):
                self.loadbalance = propValue
            else : 
                raise ValueError('invalid load_balance value: Valid values are only-rr, only-primary, prefer-rr, prefer-primary, any or true')
            sb = ClusterAwareRegex.sub('',self.originalDSN)

            TopologyAwareRegex = re.compile(r'topology_keys( )*=( )*(\S)*( )?')
            tp_string = TopologyAwareRegex.search(self.originalDSN)
            if tp_string != None :
                tp_string = tp_string.group()
                tp_parts = tp_string.split(self.EQUALS)
                tp_parts = list(filter(('').__ne__, tp_parts))
                self.placements = tp_parts[1].strip()
                sb = TopologyAwareRegex.sub('',sb)

            RefreshIntervalRegex = re.compile(r'yb_servers_refresh_interval( )*=( )*[0-9]*( )?')
            ri_string = RefreshIntervalRegex.search(self.originalDSN)
            if ri_string != None:
                ri_string = ri_string.group()
                ri_parts = ri_string.split(self.EQUALS)
                ri_parts = list(filter(('').__ne__, ri_parts))
                self.refreshInterval = int(ri_parts[1].strip())
                sb = RefreshIntervalRegex.sub('', sb)

            FailedHostTTLRegex = re.compile(r'failed_host_ttl_seconds( )*=( )*[0-9]*( )?')
            ri_string = FailedHostTTLRegex.search(self.originalDSN)
            if ri_string != None:
                ri_string = ri_string.group()
                ri_parts = ri_string.split(self.EQUALS)
                ri_parts = list(filter(('').__ne__, ri_parts))
                self.failed_host_ttl_seconds = int(ri_parts[1].strip())
                sb = FailedHostTTLRegex.sub('', sb)
                
            FallbackToTopologyKeysOnlyRegex = re.compile(r'fallback_to_topology_keys_only( )*=( )*([a-z]*[A-Z]*)*( )?')
            ri_string = FallbackToTopologyKeysOnlyRegex.search(self.originalDSN)
            if ri_string != None:
                ri_string = ri_string.group()
                ri_parts = ri_string.split(self.EQUALS)
                ri_parts = list(filter(('').__ne__, ri_parts))
                propValue = ri_parts[1].lower().strip()
                if propValue == 'true':
                    self.fallback_to_topology_keys_only = True
                sb = FallbackToTopologyKeysOnlyRegex.sub('', sb)
        return sb
        
    def processProperties(self):
        backup_dict = self.originalProperties
        if 'load_balance' in backup_dict:
            propValue = backup_dict.pop('load_balance')
            propValue = propValue.lower().strip()
            if (propValue == 'true' or propValue == 'any' or propValue == 'only-rr' or propValue == 'only-primary' or propValue == 'prefer-rr' or propValue == 'prefer-primary' or propValue == 'false'):
                self.loadbalance = propValue
            else : 
                raise ValueError('invalid load_balance value: Valid values are only-rr, only-primary, prefer-rr, prefer-primary, any or true')
            if 'topology_keys' in backup_dict:
                self.placements = backup_dict.pop('topology_keys')
            if 'yb_servers_refresh_interval' in backup_dict:
                self.refreshInterval = int(backup_dict.pop('yb_servers_refresh_interval'))
            if 'failed_host_ttl_seconds' in backup_dict:
                self.refreshInterval = int(backup_dict.pop('failed_host_ttl_seconds'))
            if 'fallback_to_topology_keys_only' in backup_dict:
                propValue = backup_dict.pop('fallback_to_topology_keys_only')
                propValue = propValue.lower().strip()
                if propValue == 'true':
                    self.fallback_to_topology_keys_only = True
        return backup_dict


    def getOriginalDSN(self):
        return self.originalDSN

    def getStrippedDSN(self):
        return self.ybDSN

    def getOriginalProperties(self):
        return self.originalProperties

    def getStrippedProperties(self):
        return self.ybProperties

    def getKey(self):
        return self.key

    def hasLoadBalance(self):
        return self.loadbalance != 'false'

    def getAppropriateLoadBalancer(self):
        if self.placements == '':
            ld = LoadBalanceProperties.CONNECTION_MANAGER_MAP.get(self.loadbalance)
            if ld == None:
                ld = ClusterAwareLoadBalancer(self.loadbalance, self.refreshInterval, self.failed_host_ttl_seconds)
                LoadBalanceProperties.CONNECTION_MANAGER_MAP[self.loadbalance] = ld
            self.key = self.loadbalance
        else:
            lbKey = ""
            if self.fallback_to_topology_keys_only:
                lbKey = self.loadbalance + "&" + self.placements + "&True"
            else:
                lbKey = self.loadbalance + "&" + self.placements + "&False"
            ld = LoadBalanceProperties.CONNECTION_MANAGER_MAP.get(lbKey)
            if ld == None :
                ld = TopologyAwareLoadBalancer(self.loadbalance, self.placements, self.refreshInterval, self.failed_host_ttl_seconds, self.fallback_to_topology_keys_only)
                LoadBalanceProperties.CONNECTION_MANAGER_MAP[lbKey] = ld
            self.key = lbKey
            ld.refreshListSeconds = self.refreshInterval if self.refreshInterval >= 0 and self.refreshInterval <= 600 else 300
            ld.failedHostsTTL = self.failed_host_ttl_seconds if self.failed_host_ttl_seconds >= 0 and self.failed_host_ttl_seconds <= 60 else 5
        return ld
    
    def getAppropriateLoadBalancerToCloseConnection(self, lb_key):
        if lb_key == 'none':
            return None
        return LoadBalanceProperties.CONNECTION_MANAGER_MAP.get(lb_key)
    

import re
from psycopg2.policies import ClusterAwareLoadBalancer,TopologyAwareLoadBalancer
class LoadBalanceProperties:

    CONNECTION_MANAGER_MAP = {}
    placements = ''
    SIMPLE_LB = 'simple'

    def __init__(self, dsn, **kwargs):
        self.SIMPLE_LB = 'simple'
        self.LOAD_BALANCE_PROPERTY_KEY = 'load_balance'
        self.EQUALS = '='
        self.originalDSN = dsn
        self.originalProperties = kwargs
        self.loadbalance = False
        self.placements = ''
        self.ybProperties = self.originalProperties
        self.ybDSN = None
        if self.originalDSN != None :
            self.ybDSN = self.processURL()
        else:
            self.ybProperties = self.processProperties()
        

    def processURL(self):
        ClusterAwareRegex = re.compile(r'load_balance( )*=( )*([a-z]*[A-Z]*)*( )?')
        lb_string = ClusterAwareRegex.search(self.originalDSN)
        if lb_string == None :
            return self.originalDSN
        else:
            lb_string = lb_string.group()
            lb_parts = lb_string.split(self.EQUALS)
            lb_parts = list(filter(('').__ne__, lb_parts))
            propValue = lb_parts[1].lower().strip()
            if propValue == 'true':
                self.loadbalance = True
            sb = ClusterAwareRegex.sub('',self.originalDSN)

            TopologyAwareRegex = re.compile(r'topology_keys( )*=( )*(\S)*( )?')
            tp_string = TopologyAwareRegex.search(self.originalDSN)
            if tp_string != None :
                tp_string = tp_string.group()
                tp_parts = tp_string.split(self.EQUALS)
                tp_parts = list(filter(('').__ne__, tp_parts))
                self.placements = tp_parts[1].strip()
                sb = TopologyAwareRegex.sub('',sb)
        return sb
        
    def processProperties(self):
        backup_dict = self.originalProperties
        if 'load_balance' in backup_dict:
            propValue = backup_dict.pop('load_balance')
            propValue = propValue.lower().strip()
            if propValue == 'true':
                self.loadbalance = True
            if 'topology_keys' in backup_dict:
                self.placements = backup_dict.pop('topology_keys')
        return backup_dict


    def getOriginalDSN(self):
        return self.originalDSN

    def getStrippedDSN(self):
        return self.ybDSN

    def getOriginalProperties(self):
        return self.originalProperties

    def getStrippedProperties(self):
        return self.ybProperties

    def hasLoadBalance(self):
        return self.loadbalance

    def getAppropriateLoadBalancer(self):
        if self.placements == '':
            ld = LoadBalanceProperties.CONNECTION_MANAGER_MAP.get(self.SIMPLE_LB)
            if ld == None:
                ld = ClusterAwareLoadBalancer.getInstance()
                LoadBalanceProperties.CONNECTION_MANAGER_MAP[self.SIMPLE_LB] = ld
        else:
            ld = LoadBalanceProperties.CONNECTION_MANAGER_MAP.get(self.placements)
            if ld == None :
                ld = TopologyAwareLoadBalancer(self.placements)
                LoadBalanceProperties.CONNECTION_MANAGER_MAP[self.placements] = ld
        return ld

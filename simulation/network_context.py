from typing import List

import simpy

from ether.topology import Topology
from simulation.processes import BrokerProcess, ClientProcess


class NetworkContext:
    broker_procs: List[BrokerProcess]
    client_procs: List[ClientProcess]
    topology: Topology

    def __init__(self, broker_procs, client_procs, topology):
        self.broker_procs = broker_procs
        self.client_procs = client_procs
        self.topology = topology

    def get_all_workers(self) -> List[BrokerProcess]:
        return self.broker_procs

    def get_workers(self, has_broker) -> List[BrokerProcess]:
        return [bp for bp in self.broker_procs if bp.running == has_broker]

    def get_idle_workers(self) -> List[BrokerProcess]:
        return self.get_workers(False)

    def get_brokers(self) -> List[BrokerProcess]:
        return self.get_workers(True)

    def get_num_clients(self) -> int:
        return len(self.client_procs)
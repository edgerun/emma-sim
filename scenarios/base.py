import abc
import logging
import random
from typing import TextIO, List

import networkx as nx
import numpy
import simpy

from ether.cell import Broker, Client
from ether.core import Connection
from ether.topology import Topology
from simulation.processes import BrokerProcess, ClientProcess, CoordinatorProcess
from simulation.protocol import Protocol


class Scenario(metaclass=abc.ABCMeta):
    name: str
    enable_ack: bool
    use_vivaldi: bool
    action_interval: int
    publish_interval: int

    broker_procs: List[BrokerProcess]
    client_procs: List[ClientProcess]
    topology: Topology
    env: simpy.Environment
    csv_file: TextIO
    protocol: Protocol
    logger: logging.Logger

    def __init__(self, name: str, enable_ack=False, verbose=False, use_vivaldi=False, action_interval=1,
                 publish_interval=100):
        self.name = name
        self.enable_ack = enable_ack
        self.use_vivaldi = use_vivaldi
        self.action_interval = action_interval
        self.publish_interval = publish_interval
        self.broker_procs = []
        self.client_procs = []
        self.env = simpy.Environment()
        self.csv_file = open(f'{name}.csv', 'w')
        self.logger = logging.getLogger('scenario')
        logging.basicConfig(force=True, filename=f'{self.name}.log', filemode='w', level=logging.DEBUG)
        if verbose:
            console_handler = logging.StreamHandler()
            logging.getLogger().addHandler(console_handler)

    def spawn_broker(self, backhaul, name: str) -> BrokerProcess:
        broker = Broker(name, backhaul=backhaul)
        broker.materialize(self.topology)
        bp = BrokerProcess(self.env, self.protocol, broker, self.broker_procs, self.use_vivaldi)
        self.env.process(bp.run())
        self.env.process(bp.run_pub_process())
        if self.use_vivaldi:
            self.env.process(bp.ping_all(lambda: map(lambda bp: bp.node, self.broker_procs)))
        self.broker_procs.append(bp)
        return bp

    def spawn_client(self, backhaul, name: str, topic: str, publishers=0, subscribe=False) -> ClientProcess:
        client = Client(name, backhaul=backhaul)
        client.materialize(self.topology)
        cp = ClientProcess(self.env, self.protocol, client, self.broker_procs[0].node, self.use_vivaldi)
        if subscribe:
            self.env.process(cp.subscribe(topic))
        self.env.process(cp.run())
        for _ in range(publishers):
            self.env.process(cp.run_publisher(topic, self.publish_interval))
        if self.use_vivaldi:
            self.env.process(cp.run_ping_loop())
        self.client_procs.append(cp)
        return cp

    def spawn_coordinator(self):
        coordinator_process = CoordinatorProcess(self.env, self.topology, self.protocol, self.client_procs,
                                                 self.broker_procs, self.use_vivaldi)
        self.topology.add_connection(Connection(coordinator_process.node, 'eu-central'))
        self.env.process(coordinator_process.run_reconnect_process())
        if not self.use_vivaldi:
            self.env.process(coordinator_process.run_monitoring_process())

    def sleep(self):
        return self.env.timeout(self.action_interval * 60_000)

    def create_initial_topology(self) -> Topology:
        topology = Topology()
        topology.load_inet_graph('cloudping')
        # maps region names of cloudping dataset to custom region names
        region_map = {
            'internet_eu-west-1': 'eu-west',
            'internet_eu-central-1': 'eu-central',
            'internet_us-east-1': 'us-east',
        }
        # remove all or regions from the graph
        topology.remove_nodes_from([n for n in topology.nodes if n not in region_map.keys()])
        # relabel the region nodes according to the map above
        nx.relabel_nodes(topology, region_map, copy=False)
        return topology

    @abc.abstractmethod
    def scenario_process(self):
        ...

    def run(self):
        random.seed(0)
        numpy.random.seed(0)

        self.topology = self.create_initial_topology()
        self.protocol = Protocol(self.env, self.topology, self.enable_ack, csv_file=self.csv_file)
        self.env.process(self.scenario_process())

        minutes = self.action_interval * 10
        for i in range(minutes):
            self.env.run((i+1) * 60_000)
            if self.logger.level > logging.DEBUG:
                continue
            if any(len(p.subscribers) > 0 for p in self.broker_procs):
                for p in self.broker_procs:
                    if len(p.subscribers) == 0:
                        continue
                    self.log(f'--- subscribers on {p.node} ---')
                    for topic, subscribers in p.subscribers.items():
                        if len(subscribers) > 0:
                            self.log(f'[{topic}] {subscribers}')
            if any(len(s.items) > 0 for s in self.protocol.stores.values()):
                self.log(f'--- message queues ---')
                for p in [*self.broker_procs, *self.client_procs]:
                    p_msgs = self.protocol.stores[p.node].items
                    counts = {t.__name__: len([m for m in p_msgs if isinstance(m, t)])
                              for t in {type(m) for m in p_msgs}}
                    if len(counts) > 0:
                        self.log(f'{p.node.name}: {counts}')

        self.csv_file.close()

    def log(self, message):
        minutes = int(self.env.now / 1000 / 60)
        seconds = int(self.env.now / 1000 % 60)
        logging.info(f'{minutes:02d}:{seconds:02d} {message}')


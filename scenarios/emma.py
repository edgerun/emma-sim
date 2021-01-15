import argparse
import logging
import sys
from collections import defaultdict
from concurrent.futures.process import ProcessPoolExecutor
from itertools import count
from os import chdir
from typing import Dict, Iterator, Optional

import networkx as nx

from ether.topology import Topology
from scenarios.base import Scenario


class EmmaScenario(Scenario):
    broker_counters: Dict[str, Iterator[int]] = defaultdict(lambda: count(1))
    client_counters: Dict[str, Iterator[int]] = defaultdict(lambda: count(1))
    clients_per_group: int
    publishers_per_client: int

    def __init__(self, name: str, use_vivaldi=False, action_interval=1, verbose=False, clients_per_group=10,
                 publishers_per_client=7, publish_interval=100, enable_ack=False):
        """
        Create a new emma scenario

        :param name: identifier used in log output and for file names
        :param use_vivaldi: run vivaldi on nodes and use coordinates to estimate distances
        :param action_interval: scenario action interval in minutes
        :param verbose: enables more verbose logging to the console
        :param clients_per_group: number of clients per client group
        :param publishers_per_client: number of publishers per client
        :param publish_interval: publish interval in milliseconds
        :param enable_ack: enable ACK messages in the protocol
        """
        super().__init__(name, enable_ack, verbose, use_vivaldi, action_interval, publish_interval)
        self.clients_per_group = clients_per_group
        self.publishers_per_client = publishers_per_client

    def spawn_cloud_broker(self, region: str):
        return self.spawn_broker(region, f'{region}_broker_{next(self.broker_counters[region])}')

    def spawn_cloud_client(self, region: str, topic: Optional[str], publishers=1, subscribe=True):
        name = f'{region}_client_{next(self.client_counters[region])}'
        return self.spawn_client(region, name, topic or region, publishers, subscribe)

    def spawn_client_group(self, region: str):
        # a client group consists of 10 VMs, each running a subscriber and 7 publishers
        for _ in range(self.clients_per_group):
            self.spawn_cloud_client(region, region, self.publishers_per_client)

    def scenario_process(self):
        self.logger.info(f'===== STARTING SCENARIO {self.name.upper()} =====')
        self.log('[0] spawn coordinator and initial broker')
        self.spawn_coordinator()
        self.spawn_cloud_broker('eu-central')
        yield self.sleep()

        self.log('[1] topic global: one publisher and subscriber in `us-east` and `eu-west`, '
                 'one subscriber in `eu-central`')
        self.spawn_cloud_client('eu-west', 'global')
        central_client = self.spawn_cloud_client('eu-central', 'global', publishers=0)
        self.spawn_cloud_client('us-east', 'global')
        yield self.sleep()

        self.log('[2] client group appears in us-east')
        self.spawn_client_group('us-east')
        yield self.sleep()

        self.log('[3] broker spawns in eu-west')
        self.spawn_cloud_broker('eu-west')
        yield self.sleep()

        self.log('[4] client group appears in eu-west')
        self.spawn_client_group('eu-west')
        yield self.sleep()

        self.log('[5] broker spawns in us-east')
        us_east_broker = self.spawn_cloud_broker('us-east')
        yield self.sleep()

        self.log('[6] broker spawns in eu-west')
        self.spawn_cloud_broker('eu-west')
        yield self.sleep()

        self.log('[7] subscriber to topic `global` in eu-central disappears')
        yield central_client.shutdown()
        yield self.sleep()

        self.log('[8] broker shuts down in us-east')
        yield us_east_broker.shutdown()

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


def main():
    logging.basicConfig(filename='emma.log', level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='print stats')
    parser.add_argument('-o', '--output', type=str, help='path to CSV output')
    parser.add_argument('--publishers-per-client', type=int, default=7)
    parser.add_argument('--publish-interval', type=int, default=100, help='publish interval in ms')
    parser.add_argument('--clients-per-group', type=int, default=10)
    parser.add_argument('--enable-ack', action='store_true')
    args = parser.parse_args(sys.argv[1:])
    if args.output:
        chdir(args.output)
    common_kwargs = {
        'publishers_per_client': args.publishers_per_client,
        'publish_interval': args.publish_interval,
        'clients_per_group': args.clients_per_group,
        'verbose': args.verbose,
        'enable_ack': args.enable_ack,
    }

    scenario_configs = [
        {
            **common_kwargs,
            'name': 'baseline',
        },
        {
            **common_kwargs,
            'name': 'vivaldi',
            'use_vivaldi': True,
        },
    ]

    def run_scenario(**kwargs):
        EmmaScenario(**kwargs).run()

    if args.verbose:
        print('running emma scenarios sequentially')
        for config in scenario_configs:
            run_scenario(**config)
    else:
        print('running emma scenarios in separate processes')
        executor = ProcessPoolExecutor(len(scenario_configs))
        for config in scenario_configs:
            executor.submit(run_scenario, **config)


if __name__ == '__main__':
    main()

import logging
from typing import List, Tuple, Optional

import math
import simpy

from simulation.network_context import NetworkContext
from simulation.processes import BrokerProcess, ClientProcess, WorkerProcess


class StartAllBrokerOrchestrationProcess:
    env: simpy.Environment
    broker_procs: List[BrokerProcess]

    def __init__(self, env: simpy.Environment, broker_procs: List[BrokerProcess]):
        self.env = env
        self.broker_procs = broker_procs

    def run(self):
        while True:
            for bp in self.broker_procs:
                if not bp.running:
                    self.env.run(bp.run)
            yield self.env.timeout(15_000)


class DistanceBasedBrokerScalerProcess:
    env: simpy.Environment
    ctx: NetworkContext

    def __init__(self, env: simpy.Environment, ctx: NetworkContext, th_up: float, th_down: float):
        self.env = env
        self.ctx = ctx
        self.th_up = th_up
        self.th_down = th_down

    def run(self):
        while True:
            worker_pressures = self.get_worker_pressures(False)
            p_min = self.calculate_min_pressure()
            p_max = self.calculate_max_pressure()

            # scale up
            if len(worker_pressures) > 0:
                candidate_worker, pressure = max(worker_pressures, key=lambda t: t[1])
                pressure = (pressure - p_min) / (p_max - p_min)
                if pressure > self.th_up:
                    self.env.process(candidate_worker.run())

            # scale down (only if more than one broker is left)
            if len(self.ctx.get_brokers()) > 1:
                candidate_worker, pressure = min(self.get_worker_pressures(True), key=lambda t: t[1])
                pressure = (pressure - p_min) / (p_max - p_min)
                if pressure < self.th_down:
                    candidate_worker.shutdown()

            yield self.env.timeout(60_000)

    def get_worker_pressures(self, has_broker: bool) -> List[Tuple[WorkerProcess, float]]:
        pressures = []
        for worker in self.ctx.get_workers(has_broker):
            pressure = 0
            for _, distance in self.ctx.get_client_distances(worker):
                pressure += 1 / distance
            pressures.append((worker, pressure))
        return pressures

    def calculate_max_pressure(self):
        return self.ctx.get_num_clients()

    def calculate_min_pressure(self):
        min_pressure = int('Inf')
        for worker in self.ctx.get_all_workers():
            pressure = 1 / max(self.ctx.get_client_distances(worker), lambda t: t[1])
            if pressure < min_pressure:
                min_pressure = pressure
        return min_pressure


class DistanceDiffBrokerScalerProcess:
    env: simpy.Environment
    ctx: NetworkContext

    def __init__(self, env: simpy.Environment, context: NetworkContext, th_up: float, th_down: float):
        self.env = env
        self.ctx = context
        self.th_up = th_up
        self.th_down = th_down
        self.logger = logging.getLogger()

    def run(self):
        while True:
            # check if we have to scale up
            worker_pressures = self.get_worker_pressures()
            if len(worker_pressures) > 0:
                candidate_worker, pressure = max(worker_pressures, key=lambda t: t[1])
                if pressure > self.th_up:
                    candidate_worker.start_broker()

            broker_pressures = self.get_broker_pressures()
            if len(self.ctx.get_brokers()) > 1:
                candidate_broker, pressure = min(broker_pressures, key=lambda t: t[1])
                if pressure < self.th_down:
                    candidate_broker.shutdown()

            yield self.env.timeout(60_000)

    def get_worker_pressures(self) -> List[Tuple[WorkerProcess, float]]:
        """
        Pressure for scale-up (if pressure is high, add broker)

        :return: a list of scalar pressure values for each worker
        """
        return [(worker, self.calc_worker_pressure(worker)) for worker in self.ctx.get_idle_workers()]

    def get_broker_pressures(self) -> List[Tuple[BrokerProcess, float]]:
        """
        Pressure for scale-down (if pressure is low, remove broker)

        :return: a list of scalar pressure values for each broker
        """
        return [(broker, self.calc_broker_pressure(broker)) for broker in self.ctx.get_brokers()]

    def calc_worker_pressure(self, worker: WorkerProcess) -> float:
        """
        Calculates a pressure that uses as proxy the potential improvement in distance (latency) for clients, if a
        broker were scaled to the given worker. It works under the assumption that clients will be connected to the
        closest broker.

        :param worker: the candidate worker
        :return: a scalar value estimating the goodness of spawning a broker on this worker
        """
        # alternatively: calculate the average, or the % improvement, ...

        pressure = 0
        for _, distance_to_worker, distance_to_connected_broker in self._get_worker_pressure_tuples(worker):
            # if the broker is 100 away, and the candidate worker 10, the improvement for the client would be 90
            pressure += (distance_to_connected_broker - distance_to_worker)

        return math.log(pressure) if pressure > 0 else 0

    def calc_broker_pressure(self, broker: BrokerProcess) -> float:
        """
        Naively: estimate how bad it would be to remove this broker. Builds on the assumption that, if this broker were
        to be removed, the clients would be connected to the next-closest broker. If the pressure is low (proxy for
        latency penalty), then we can remove the broker.

        :param broker: the candidate broker to be removed
        :return: a scalar value estimating the goodness of removing this broker
        """
        # alternatively: calculate the average, or the % improvement, ...
        pressure = 0
        for _, distance_to_broker, distance_to_next in self._get_broker_pressure_tuples(broker):
            # if the current broker is 10 away, and the next is 100 away, the penalty would be 90.
            pressure += (distance_to_next - distance_to_broker)

        return math.log(pressure) if pressure > 0 else 0

    def _get_broker_pressure_tuples(self, broker: BrokerProcess) -> List[Tuple[ClientProcess, float, float]]:
        def get_next_broker_distance(c: ClientProcess) -> Optional[Tuple[BrokerProcess, float]]:
            brokers = [(b, b.node.distance_to(c.node)) for b in self.ctx.get_brokers() if b.node != broker.node]
            return min(brokers, key=lambda bd: bd[1]) if len(brokers) > 0 else None

        client_proximity = self.ctx.get_client_distances(broker.wp)
        potentials = list()

        for client, distance_to_here in client_proximity:
            if client.selected_broker != broker.node:
                continue

            next_broker_distance = get_next_broker_distance(client)
            if next_broker_distance is None:
                continue

            _, distance_to_next = next_broker_distance
            if distance_to_here >= distance_to_next:
                # client is connected to a broker that's closer than the candidate
                continue

            potentials.append((client, distance_to_here, distance_to_next))

        return potentials

    def _get_worker_pressure_tuples(self, worker: WorkerProcess) -> List[Tuple[ClientProcess, float, float]]:
        """
        :param worker: the candidate worker
        :return: a list of tuples with: the client, the distance to the worker, the distance to the connected broker
        """
        client_proximity = self.ctx.get_client_distances(worker)
        potentials = list()

        for client, distance_to_here in client_proximity:
            distance_to_broker = client.node.distance_to(client.selected_broker)
            if distance_to_here >= distance_to_broker:
                # client is connected to a broker that's closer than the candidate worker
                continue

            potentials.append((client, distance_to_here, distance_to_broker))

        return potentials

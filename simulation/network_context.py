from simulation.processes import *


class NetworkContext:
    client_procs: List[ClientProcess]
    worker_procs: List[WorkerProcess]
    topology: Topology

    def __init__(self, topology):
        self.client_procs = []
        self.worker_procs = []
        self.topology = topology

    def add_client(self, cp: ClientProcess):
        self.client_procs.append(cp)

    def add_worker(self, wp: WorkerProcess):
        self.worker_procs.append(wp)

    def get_all_workers(self) -> List[WorkerProcess]:
        return self.worker_procs

    def get_workers(self, has_broker) -> List[WorkerProcess]:
        return [wp for wp in self.worker_procs if wp.bp.running == has_broker]

    def get_idle_workers(self) -> List[WorkerProcess]:
        return self.get_workers(False)

    def get_brokers(self) -> List[BrokerProcess]:
        return [wp.bp for wp in self.get_workers(True)]

    def get_num_clients(self) -> int:
        return len(self.client_procs)

    def get_clients(self) -> List[ClientProcess]:
        return [cp for cp in self.client_procs if cp.running]

    def get_client_distances(self, worker: WorkerProcess) -> List[Tuple[ClientProcess, float]]:
        return [(client, worker.node.distance_to(client.node)) for client in self.get_clients()]

    def get_closest_brokers(self, node: Node) -> List[BrokerProcess]:
        return sorted(self.get_brokers(), key=lambda bp: node.distance_to(bp.node))

# Individual Submission:
# Name: Dhanraj Shrivastav
# Roll No: CS24MTECH11004
###########################

from threading import Thread

class Vertex:
    """
    A Pregel Vertex class
    """
    def __init__(self, id, value, out_neighbours, num_vertices, agg_list):
        self.id = id
        self.value = value
        self.out_neighbours = out_neighbours
        self.incoming_messages = []
        self.outgoing_messages = []
        self.active = True
        self.superstep = 0
        self.agg_list = agg_list
        self.num_vertices = num_vertices

class Aggregator:
    """
    Pregel aggregator used to compute a value and broadcast result
    to all nodes.
    """
    def __init__(self, init_value = None):
        self.values = []
        self.result = init_value

    def push(self, value):
        """
        This method allows a send to send a value to the aggregator for computation.
        """
        self.values.append(value)

    def pull(self):
        """
        This method allows a vertex to see what value the aggregator has computed.
        """
        return self.result        

class Pregel:
    """
    Pregel framework implemented
    """
    def __init__(self, vertices, nb_workers, agg_list):
        """
        A simple constructor
        """
        self.vertices = vertices
        self.nb_workers = nb_workers
        self.agg_list = agg_list
        self.assign_workers()

    def assign_workers(self):
        """
        Partition the vertices between workers
        """
        self.worker_assignment = {worker_id : [] for worker_id in range(self.nb_workers)}
        for vertex in self.vertices:
            self.worker_assignment[hash(str(vertex.id)) % self.nb_workers].append(vertex)

    def run(self):
        """
        Run pregel
        """
        while sum([vert.active for vert in self.vertices]) != 0:
            self.superstep()
            self.propagate_messages()

    def superstep(self):
        """
        A Pregel superstep
        """
        worker_list = [Thread(target=run_worker, args=(self.worker_assignment[i],)) for i in range(self.nb_workers)]
        for worker in worker_list:
            worker.start()
        for worker in worker_list:
            worker.join()
        for agg in self.agg_list:
            agg.reduction()

    def propagate_messages(self):
        """
        Propagate messages through edges
        """
        for vertex in self.vertices:
            vertex.superstep += 1
            vertex.incoming_messages = []
        for vertex in self.vertices:    
            for (rcv_vertex, message) in vertex.outgoing_messages:
                rcv_vertex.incoming_messages.append((vertex, message))

def run_worker(vertices):
    """
    Run the worker
    """
    for vertex in vertices:
        if vertex.active:
            vertex.update()

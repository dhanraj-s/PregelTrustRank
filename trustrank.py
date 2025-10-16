# Individual Submission:
# Name: Dhanraj Shrivastav
# Roll No: CS24MTECH11004
###########################

from pregel import Vertex, Aggregator
import numpy as np

class SumAggregator(Aggregator):
    """
    Inherits from Aggregator.
    Computes the sum of values sent to it, and broadcasts to all vertices.
    """
    def __init__(self, init_value=None):
        super().__init__(init_value)
    def reduction(self):
        self.result = sum(self.values)
        self.values = []

class MeanAggregator(Aggregator):
    """
    Inherits from Aggregator.
    Computes the mean of all values sent to it, and broadcasts to all vertices.
    """
    def __init__(self, init_value=None):
        super().__init__(init_value)
    def reduction(self):
        if len(self.values):
            self.result = sum(self.values) / len(self.values)
        self.values = []

class TrustRankVertex(Vertex):
    """
    Inherits from Pregel Vertex.
    Overrides the update method.
    """
    def __init__(self, id, value, out_neighbours, num_vertices, num_fraud_nodes, agg_list, fraud, max_iter = 100,\
                 tol = 1e-9):
        super().__init__(id, value, out_neighbours, num_vertices, agg_list)
        self.fraud = fraud
        self.num_fraud_nodes = num_fraud_nodes
        self.max_iter = max_iter
        self.tol = tol
    
    def update(self):
        """
        Update step. Handles the case where a node has no outgoing edges.
        Approach: Assume that a node with no outgoing edges has an outgoing link to every other vertex (including itself).
        Therefore, such a vertex distributes its score equally to all vertices (in case of usual PageRank).
        And, in case of TrustRank, it distributes its score equally among only the fraud vertices.
        Intuition is a tweak of the random surfer model. A fraud sender will only teleport to fraud nodes with equal probability.
        """
        if self.superstep < self.max_iter:
            
            # pulls value from the sum aggregator to obtain the sum of trustranks of all sink nodes
            # see the update equation in the report for explanation.
            sink_sum = self.agg_list[0].pull()

            # trustrank update (this is basically personalized pagerank)
            old_value = self.value
            self.value = 0.85 * sum([trustrank for _, trustrank in self.incoming_messages])\
                    + 0.85 * sink_sum * (1/self.num_fraud_nodes if self.fraud else 0)\
                    + 0.15 * (1/self.num_fraud_nodes if self.fraud else 0)
            
            # this pushes the change in value of current node to a mean aggregator
            # the mean aggregator will compute the mean absolute deviation
            # this will be used to determine the stopping criterion.
            self.agg_list[1].push(abs(old_value - self.value))
            if (self.superstep > 0) and (self.agg_list[1].pull() < self.tol):
                self.active = False

            elif len(self.out_neighbours) != 0:
                outgoing_trustrank = self.value / len(self.out_neighbours)
                self.outgoing_messages = [(vertex, outgoing_trustrank) for vertex in self.out_neighbours]
            else:
                # a sink node will push its value to the sum aggregator
                # this will broadcast it to all nodes.
                self.agg_list[0].push(self.value)
        else:
            # deactivate vertex if max_iters crossed.
            self.active = False

def num_sink_nodes(vertices):
    return len([sink_vertex for sink_vertex in vertices if len(sink_vertex.out_neighbours) == 0])


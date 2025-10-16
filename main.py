# Individual Submission:
# Name: Dhanraj Shrivastav
# Roll No: CS24MTECH11004
###########################

import pandas as pd
import numpy as np
from trustrank import TrustRankVertex, SumAggregator, MeanAggregator, num_sink_nodes
from pregel import Pregel
import matplotlib.pyplot as plt     # used for plotting data

def load_data():
    """
    Loads the payment data and data about bad senders
    """
    print('Loading \'Payments.xlsx\'...')
    payment_data = pd.read_excel('Payments.xlsx')
    print('\'Payments.xlsx\' loaded!')

    print('Loading \'bad_sender.xlsx\'...')
    fraud_nodes = pd.read_excel('bad_sender.xlsx')
    print('\'bad_sender.xlsx\' loaded!')

    return (payment_data, fraud_nodes)

def build_graph(payment_data, fraud_nodes): 
    """
    Builds the graph from the given data
    """
    print()
    print('Building graph...')
    
    all_nodes = list(set(payment_data.Sender.tolist() + payment_data.Receiver.tolist()))
    node_to_index = dict(zip(all_nodes, list(range(len(all_nodes)))))
    fraud_nodes = list(set(fraud_nodes['Bad Sender'].tolist()))

    vertices = [TrustRankVertex(i, 1/len(all_nodes), [], len(all_nodes), len(fraud_nodes),\
                             [], True if i in fraud_nodes else False) for i in all_nodes]
    
    for v in vertices:
        vertices[node_to_index[v.id]].out_neighbours = \
            [vertices[node_to_index[id]] for id in pd.unique(payment_data[payment_data.Sender == v.id].Receiver).tolist()]
    print('Graph built!')
    print()
    return vertices

def trustrank(vertices, nb_workers = 10, max_iter = 100, tol = 1e-6):
    """
    Runs Pregel based trustrank
    """
    sink_agg = SumAggregator( (1/len(vertices)) * num_sink_nodes(vertices) )
    error_agg = MeanAggregator()
    
    for v in vertices:
        v.agg_list = [sink_agg, error_agg]
        v.max_iter = max_iter
        v.tol = tol

    p = Pregel(vertices, nb_workers, [sink_agg, error_agg])
    p.run()

    res = dict(zip([v.id for v in vertices], [v.value for v in vertices]))
    return(dict(sorted(res.items(), key=lambda item: item[1], reverse=True)))

def plot_data(tr_dict, fraud_dict):
    """
    This function plots a bar graph of sender Id vs trustrank score
    Also plots histogram of trustrank scores
    """

    # This plots sender ID vs Trustrank score (bar graph)
    _, ax_all = plt.subplots()
    ax_all.bar(tr_dict.keys(), tr_dict.values())
    ax_all.set_xlabel('Sender ID')
    ax_all.set_ylabel('Trust Score')
    ax_all.set_title('Trust score bar graph for all senders')


    # Plots sender ID vs Trustrank score (bar graph) for only the fraud nodes
    _, ax_fraud = plt.subplots()
    ax_fraud.bar(fraud_dict.keys(), fraud_dict.values())
    ax_fraud.set_xlabel('Fraud sender ID')
    ax_fraud.set_ylabel('Trust score')
    ax_fraud.set_title('Trust score for fraud nodes')

    # Plots trustrank score histogram
    _, ax_hist = plt.subplots()
    ax_hist.hist(tr_dict.values())
    ax_hist.set_xlabel('Trustrank Scores')
    ax_hist.set_ylabel('Counts')
    ax_hist.set_title('Trustrank score histogram')

    plt.show()

if __name__ == '__main__':
    # Load data from given datasets
    payment_data, fraud_nodes = load_data()

    # Compute trustrank scores
    tr = trustrank(build_graph(payment_data, fraud_nodes), tol=1e-9)
    
    # Print trustrank scores for all nodes
    print('TrustRank Scores:')
    print('(SenderId, Score)')
    print()
    for x in tr.items():
        print(x)

    print()

    # Trustrank scores of only fraud nodes
    print('Scores of nodes known to be fraud:')
    fraud = fraud_nodes['Bad Sender'].tolist()
    for f in fraud:
        print((f, tr[f]))

    # Generate plots
    plot_data(tr, dict(zip(fraud, [tr[f] for f in fraud])))
    
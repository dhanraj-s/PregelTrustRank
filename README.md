# PregelTrustRank

This repository contains an implementation of the Trust Rank algorithm following [Google's Pregel Framework](https://research.google/pubs/pregel-a-system-for-large-scale-graph-processing/)

The ground truth fraudulent senders are listed in the file bad_sender.xlsx
The transaction dataset is Payments.xlsx

Modifies the Pregel implementation given [here](https://michaelnielsen.org/ddi/pregel/) by adding Aggregators to compute trust rank scores.

Simply create a virtual environment. Install the requirements. And run the script 'main.py':

The Trust Rank scores of known fraud nodes are high:

<img width="374" height="451" alt="image" src="https://github.com/user-attachments/assets/e4165a20-55fa-4598-aa7b-33243b48ba6e" />


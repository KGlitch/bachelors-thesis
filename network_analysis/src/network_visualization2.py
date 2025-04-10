import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import os
import numpy as np
import community as community_louvain  # Louvain Methode

# Define directories
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, 'network_analysis_results')
OUTPUT_DIR = DATA_DIR

# Load the graph from GraphML file
G = nx.read_graphml(os.path.join(DATA_DIR, 'company_network.graphml'))

# Convert the graph to undirected if it isn't already
if nx.is_directed(G):
    G = G.to_undirected()

# Community Detection
partition = community_louvain.best_partition(G)

# Group nodes by communities
communities = {}
for node, comm in partition.items():
    communities.setdefault(comm, []).append(node)

# Position communities in a circle
community_pos = nx.circular_layout(range(len(communities)), scale=15)

# Position nodes within their communities
pos = {}
for comm, nodes in communities.items():
    subgraph = G.subgraph(nodes)
    sub_pos = nx.spring_layout(subgraph, k=2, iterations=300, seed=42)
    
    # Move nodes to their community center
    center_x, center_y = community_pos[comm]
    for node, (x, y) in sub_pos.items():
        pos[node] = (x + center_x, y + center_y)

# Plotting
plt.figure(figsize=(40, 30))
colors = [partition[node] for node in G.nodes()]
nx.draw_networkx(
    G,
    pos=pos,
    with_labels=True,
    node_color=colors,
    cmap=plt.cm.tab10,
    node_size=200,
    font_size=6,
    edge_color='grey',
    alpha=0.7,
    width=0.5
)
plt.axis('off')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'network_visualization_clustered.png'))
plt.show()

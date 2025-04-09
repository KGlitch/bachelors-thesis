import networkx as nx
import matplotlib.pyplot as plt
import json
import pandas as pd
import os
import numpy as np
import community  # Import the Louvain method package

# Define paths relative to the script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', '..', 'data')  # Updated path to point to the root data directory
OUTPUT_DIR = os.path.join(SCRIPT_DIR, '..', 'network_analysis_results')

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Load data
nodes_df = pd.read_csv(os.path.join(DATA_DIR, 'nodes.csv'))
edges_df = pd.read_csv(os.path.join(DATA_DIR, 'edges.csv'))

# Create an undirected graph
G = nx.Graph()

# Add nodes from the nodes DataFrame
for index, row in nodes_df.iterrows():
    G.add_node(row['company_name'], level=row['level'])

# Add edges from the edges DataFrame
for index, row in edges_df.iterrows():
    G.add_edge(row['source_company'], row['target_company'])

# Save the graph in GraphML format
nx.write_graphml(G, os.path.join(OUTPUT_DIR, 'company_network.graphml'))

# Set up the plot
plt.figure(figsize=(15, 10))

# First pass with spring layout
pos = nx.spring_layout(G, 
                      k=2.0,  # Increased optimal distance between nodes
                      iterations=200,  # More iterations for better convergence
                      scale=3.0,  # Increased scale for more spread
                      seed=42)

# Second pass with force-directed layout to further improve spacing
pos = nx.spring_layout(G, 
                      pos=pos,  # Use previous positions as starting point
                      k=3.0,  # Even larger optimal distance
                      iterations=100,
                      scale=3.0,
                      seed=42)

# Calculate node sizes based on degree, but cap the maximum size
max_degree = max(dict(G.degree()).values())
node_sizes = [min(G.degree(node) * 50, 1000) for node in G.nodes()]

# Detect communities using the Louvain method
partition = community.best_partition(G)

# Process community assignments
community_dict = {}
for node, community_id in partition.items():
    if community_id not in community_dict:
        community_dict[community_id] = []
    community_dict[community_id].append(node)

# Sort communities by size
sorted_communities = sorted(community_dict.items(), key=lambda x: len(x[1]), reverse=True)

# Save community information to a separate file
with open(os.path.join(OUTPUT_DIR, 'community_assignments.txt'), 'w') as f:
    f.write("Community Assignments:\n\n")
    for community_id, nodes in sorted_communities:
        f.write(f"Community {community_id} ({len(nodes)} nodes):\n")
        for node in sorted(nodes):
            f.write(f"  - {node}\n")
        f.write("\n")

# Add community as a node attribute for visualization
for node, community_id in partition.items():
    G.nodes[node]['community'] = community_id

# Set node colors based on community
node_colors = [G.nodes[node]['community'] for node in G.nodes()]

# Draw the network with community colors
nx.draw(G, pos,
        with_labels=True,
        node_color=node_colors,
        node_size=node_sizes,
        font_size=8,
        font_weight='bold',
        arrows=False,
        edge_color='gray',
        width=1,
        alpha=0.8)

# Add legend for communities
from matplotlib.lines import Line2D
legend_elements = [Line2D([0], [0], marker='o', color='w', label=f'Community {i}',
                         markerfacecolor=plt.cm.tab20(i % 20), markersize=10)
                  for i in range(len(community_dict))]
plt.legend(handles=legend_elements, title='Communities', bbox_to_anchor=(1.05, 1), loc='upper left')

# Add title
plt.title('Company Partnership Network', pad=20, size=16)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Save the plot
plt.savefig(os.path.join(OUTPUT_DIR, 'company_network.png'), dpi=300, bbox_inches='tight')
plt.close()

# Print only essential information
print("Network analysis completed successfully.")
print(f"Output files saved in: {OUTPUT_DIR}")
print(f"NetworkX version: {nx.__version__}") 
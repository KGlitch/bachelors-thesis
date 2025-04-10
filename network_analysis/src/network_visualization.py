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

# Export adjacency matrix
adj_matrix = nx.adjacency_matrix(G).todense()
nodes = list(G.nodes())

# Save as CSV with node labels
adj_df = pd.DataFrame(adj_matrix, index=nodes, columns=nodes)
adj_df.to_csv(os.path.join(OUTPUT_DIR, 'adjacency_matrix.csv'))

# Save as numpy array
np.save(os.path.join(OUTPUT_DIR, 'adjacency_matrix.npy'), adj_matrix)

# Save node labels separately
with open(os.path.join(OUTPUT_DIR, 'node_labels.txt'), 'w') as f:
    for node in nodes:
        f.write(f"{node}\n")

# Set up the plot
plt.figure(figsize=(20, 15))

# Detect communities using the Louvain method
partition = community.best_partition(G)

# Calculate positions for each community separately and then combine
pos = {}
communities = set(partition.values())
community_graphs = {i: nx.Graph() for i in communities}

# Separate nodes by community
for node, comm in partition.items():
    community_graphs[comm].add_node(node)
    
for edge in G.edges():
    comm1 = partition[edge[0]]
    comm2 = partition[edge[1]]
    if comm1 == comm2:  # If nodes are in the same community
        community_graphs[comm1].add_edge(*edge)

# Position each community separately
for comm in communities:
    if len(community_graphs[comm].nodes()) > 0:
        # Get position for this community
        comm_pos = nx.spring_layout(community_graphs[comm], k=4.0, iterations=300, seed=42)
        pos.update(comm_pos)

# Arrange communities in a circular layout
num_communities = len(communities)
R = 4.0  # Radius of the circle on which to place communities
for comm in communities:
    # Calculate the center for this community
    theta = 2 * np.pi * list(communities).index(comm) / num_communities
    center = np.array([R * np.cos(theta), R * np.sin(theta)])
    
    # Move all nodes in this community
    comm_nodes = [node for node in G.nodes() if partition[node] == comm]
    if comm_nodes:
        # Calculate current center of mass for this community
        com_center = np.mean([pos[node] for node in comm_nodes], axis=0)
        # Move nodes to new position
        for node in comm_nodes:
            pos[node] = pos[node] - com_center + center

# Calculate node sizes based on degree, but with a more reasonable scale
max_degree = max(dict(G.degree()).values())
node_sizes = [min(G.degree(node) * 25, 1500) for node in G.nodes()]

# Set node colors based on community
node_colors = [partition[node] for node in G.nodes()]

# Draw the network with community colors
nx.draw(G, pos,
        with_labels=True,
        node_color=node_colors,
        node_size=node_sizes,
        font_size=8,
        font_weight='normal',
        arrows=False,
        edge_color='gray',
        width=0.5,
        alpha=0.8,
        cmap=plt.cm.tab20)

# Add legend for communities
from matplotlib.lines import Line2D
legend_elements = [Line2D([0], [0], marker='o', color='w', label=f'Community {i}',
                         markerfacecolor=plt.cm.tab20(i % 20), markersize=10)
                  for i in sorted(communities)]
plt.legend(handles=legend_elements, title='Communities', bbox_to_anchor=(1.05, 1), loc='upper left')

# Add title
#plt.title('Company Partnership Network', pad=20, size=16)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Save the plot
plt.savefig(os.path.join(OUTPUT_DIR, 'company_network.png'), dpi=300, bbox_inches='tight')
plt.close()

# Print only essential information
print("Network analysis completed successfully.")
print(f"Output files saved in: {OUTPUT_DIR}")
print(f"NetworkX version: {nx.__version__}")
print("\nGenerated files:")
print(f"- company_network.png (Network visualization)")
print(f"- company_network.graphml (GraphML format)")
print(f"- adjacency_matrix.csv (Adjacency matrix with labels)")
print(f"- adjacency_matrix.npy (Adjacency matrix in numpy format)")
print(f"- node_labels.txt (List of node labels)")
print(f"- community_assignments.txt (Community information)") 
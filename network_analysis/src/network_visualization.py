import networkx as nx
import matplotlib.pyplot as plt
import json
import pandas as pd
import os
import numpy as np
import community  # Import the Louvain method package


print("Aktuelles Arbeitsverzeichnis:", os.getcwd())


nodes_df = pd.read_csv('/Users/konrad/Documents/DHBW/Bachelorarbeit/data/nodes.csv')
edges_df = pd.read_csv('/Users/konrad/Documents/DHBW/Bachelorarbeit/data/edges.csv')

# Create an undirected graph
G = nx.Graph()  # Change to undirected graph

# Add nodes from the nodes DataFrame
for index, row in nodes_df.iterrows():
    G.add_node(row['company_name'], level=row['level'])  # Store level as node attribute

# Add edges from the edges DataFrame
for index, row in edges_df.iterrows():
    G.add_edge(row['source_company'], row['target_company'])

# Save the graph in GraphML format
nx.write_graphml(G, 'company_network.graphml')

# Set up the plot
plt.figure(figsize=(15, 10))

# Use spring layout for positioning nodes
pos = nx.spring_layout(G, iterations=50)  # Increase iterations

# Collision detection and adjustment
for node1 in G.nodes():
    for node2 in G.nodes():
        if node1 != node2:
            while np.linalg.norm(np.array(pos[node1]) - np.array(pos[node2])) < 0.1:  # Adjust threshold as needed
                pos[node2] = (pos[node2][0] + np.random.uniform(-0.1, 0.1), pos[node2][1] + np.random.uniform(-0.1, 0.1))

# Calculate node sizes based on degree
node_sizes = [G.degree(node) * 100 for node in G.nodes()]  # Scale degree for visibility

# Detect communities using the Louvain method
partition = community.best_partition(G)

# Add community as a node attribute for visualization
for node, community_id in partition.items():
    G.nodes[node]['community'] = community_id

# Set node colors based on community
node_colors = [G.nodes[node]['community'] for node in G.nodes()]

# Draw the network with community colors
nx.draw(G, pos,
        with_labels=True,
        node_color=node_colors,  # Use community colors
        node_size=node_sizes,
        font_size=8,
        font_weight='bold',
        arrows=False,
        edge_color='gray',
        width=1)

# Add title
plt.title('Company Partnership Network', pad=20, size=16)

# Adjust layout to prevent label cutoff
plt.tight_layout()


# Save the plot
plt.savefig('company_network.png', dpi=300, bbox_inches='tight')
plt.close()

print("Network visualization has been saved as 'company_network.png'")
print("Graph has been saved as 'company_network.graphml'")

print(nx.__version__) 
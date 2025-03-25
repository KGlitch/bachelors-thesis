import networkx as nx
import matplotlib.pyplot as plt
import json
import pandas as pd

# Read the JSON data
with open('../../data/partnership_articles.json', 'r') as f:
    data = json.load(f)

# Create a directed graph
G = nx.DiGraph()

# Add nodes and edges from the data
for article in data:
    # Extract companies from the article
    companies = article.get('companies', [])
    
    # Add companies as nodes
    for company in companies:
        G.add_node(company)
    
    # Add edges between companies in the same article
    for i in range(len(companies)):
        for j in range(i + 1, len(companies)):
            G.add_edge(companies[i], companies[j])
            G.add_edge(companies[j], companies[i])  # Make it undirected

# Set up the plot
plt.figure(figsize=(15, 10))

# Use spring layout for better visualization
pos = nx.spring_layout(G, k=1, iterations=50)

# Draw the network
nx.draw(G, pos,
        with_labels=True,
        node_color='lightblue',
        node_size=2000,
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
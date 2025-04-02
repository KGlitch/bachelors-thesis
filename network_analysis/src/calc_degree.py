import pandas as pd

# Pfade zu CSV-Dateien
nodes_path = "data/nodes.csv"
edges_path = "data/edges.csv"

# Dateien einlesen
nodes_df = pd.read_csv(nodes_path)
edges_df = pd.read_csv(edges_path)

# Berechne Degree für ungerichteten Graphen
undirected_edges = pd.concat([
    edges_df[['source_company']].rename(columns={'source_company': 'company_name'}),
    edges_df[['target_company']].rename(columns={'target_company': 'company_name'})
])

node_degrees = undirected_edges['company_name'].value_counts().reset_index()
node_degrees.columns = ['company_name', 'degree']

# Degree in nodes.csv einfügen (ersetzen oder hinzufügen)
nodes_df = nodes_df.drop(columns=['degree'], errors='ignore')
nodes_df = nodes_df.merge(node_degrees, on='company_name', how='left')
nodes_df['degree'] = nodes_df['degree'].fillna(0).astype(int)

# Ergebnis zurück in nodes.csv schreiben
nodes_df.to_csv(nodes_path, index=False)

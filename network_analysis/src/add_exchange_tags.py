import pandas as pd
import os

def main():
    # Read all CSV files
    edges_df = pd.read_csv('data/edges.csv')
    classification_df = pd.read_csv('data/exchange_classification.csv')
    nodes_df = pd.read_csv('data/nodes.csv')
    
    # Create a mapping of company names to their types from nodes.csv
    company_type_mapping = dict(zip(nodes_df['company_name'], nodes_df['type']))
    
    # Create a mapping of partner type combinations to exchange tags
    exchange_mapping = {}
    for _, row in classification_df.iterrows():
        key = (row['partner_1'], row['partner_2'])
        exchange_mapping[key] = row['exchange_tag']
        # Also add the reverse mapping since the relationship is bidirectional
        key_reverse = (row['partner_2'], row['partner_1'])
        exchange_mapping[key_reverse] = row['exchange_tag']
    
    # Function to get exchange tag based on partner types
    def get_exchange_tag(row):
        # Skip if exchange_tags is already filled
        if pd.notna(row['exchange_tags']) and row['exchange_tags'] != '':
            return row['exchange_tags']
            
        # Get partner types from the company names
        partner_type_1 = company_type_mapping.get(row['source_company'], '')
        partner_type_2 = company_type_mapping.get(row['target_company'], '')
        
        if not partner_type_1 or not partner_type_2:
            return ''
            
        # Try to find a matching exchange tag
        key = (partner_type_1, partner_type_2)
        if key in exchange_mapping:
            return exchange_mapping[key]
        return ''
    
    # Apply the function to update exchange tags
    edges_df['exchange_tags'] = edges_df.apply(get_exchange_tag, axis=1)
    
    # Save the updated dataframe back to CSV
    edges_df.to_csv('data/edges.csv', index=False)
    print("Exchange tags have been updated in edges.csv")

if __name__ == "__main__":
    main() 
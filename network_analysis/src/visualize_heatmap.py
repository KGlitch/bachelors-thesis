import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# Read the adjacency matrix
df = pd.read_csv('network_analysis/network_analysis_results/adjacency_matrix.csv', index_col=0)

# Create a custom colormap with white and SAP blue
colors = ['white', '#00baf2']  # White for 0, SAP blue for 1
custom_cmap = mcolors.ListedColormap(colors)

# Set font sizes
plt.rcParams['font.size'] = 12  # Base font size
plt.rcParams['axes.labelsize'] = 14  # Label font size

# Create a figure with a large size and adjust bottom margin for labels
plt.figure(figsize=(35, 30))  # Made figure wider and adjusted height

# Create heatmap using seaborn
sns.heatmap(df, 
            cmap=custom_cmap,  # Use custom colormap
            square=True,     # Make cells square
            cbar=False,      # Remove colorbar since we only have binary values
            xticklabels=True,
            yticklabels=True)

# Rotate labels
plt.xticks(rotation=90, ha='center') 
plt.yticks(rotation=0)

# Increase label font sizes
plt.tick_params(axis='both', which='major', labelsize=10)

# Add title
#plt.title('Data Ecosystem Connection Heatmap', pad=20, size=20)

# Adjust layout to prevent label cutoff
plt.tight_layout()

# Save the plot with extra margin at the bottom
plt.savefig('network_analysis/network_analysis_results/heatmap.png', 
            dpi=300, 
            bbox_inches='tight',
            pad_inches=1)
plt.close()

print("Heatmap has been saved as 'heatmap.png' in the network_analysis_results directory.") 
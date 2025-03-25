# Company Partnership Network Analysis

This project consists of two main components:

1. **News Scraper**: Collects partnership news articles from various sources
2. **Network Analysis**: Visualizes and analyzes the company partnership network

## Project Structure

```
.
├── data/                   # Shared data files
│   ├── partnership_articles.json
│   └── partnership_articles.csv
│
├── scraper/               # News scraping component
│   ├── src/              # Source code
│   │   ├── scraper.py    # Main scraper script
│   │   └── scraper.log   # Scraper logs
│   ├── requirements.txt  # Scraper dependencies
│   └── README.md        # Scraper documentation
│
└── network_analysis/     # Network visualization component
    ├── src/             # Source code
    │   └── network_visualization.py
    ├── requirements.txt # Network analysis dependencies
    └── README.md       # Network analysis documentation
``` 
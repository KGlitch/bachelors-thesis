# Partnership News Scraper

A Python-based web scraper that collects partnership and collaboration news from major technology companies. The scraper focuses on articles related to data platforms, cloud services, and strategic partnerships.

## Features

- Scrapes news from multiple technology companies (SAP, Salesforce, Snowflake, etc.)
- Filters articles by date (2021 and later)
- Prevents duplicate article collection
- Saves results in both JSON and CSV formats
- Handles various date formats and article structures
- Robust error handling and logging

## Requirements

- Python 3.8+
- Chrome browser
- ChromeDriver (compatible with your Chrome version)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/partnership-scraper.git
cd partnership-scraper
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the scraper:
```bash
python scraper.py
```

The scraper will:
1. Process each company's news pages
2. Extract relevant articles
3. Save results to `partnership_articles.json` and `partnership_articles.csv`

## Output Format

Articles are saved with the following information:
- Company name
- Article title
- URL
- Publication date
- Matched search terms
- Text snippet

## Search Terms

The scraper looks for articles containing these terms:
- partnership
- integration
- data sharing
- data platform
- cloud platform
- data lake
- data warehouse
- collaboration
- joint solution
- strategic alliance

## License

MIT License - feel free to use this project for your own purposes. 
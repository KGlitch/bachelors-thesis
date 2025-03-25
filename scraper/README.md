# News Scraper

This component handles the collection of partnership news articles from various sources.

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the scraper:
```bash
python src/scraper.py
```

The scraper will collect partnership articles and save them in the `data/` directory:
- `data/partnership_articles.json`
- `data/partnership_articles.csv` 
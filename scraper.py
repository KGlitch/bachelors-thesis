import os
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import pandas as pd
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from newspaper import Article
from tqdm import tqdm
from dotenv import load_dotenv
from selenium.common.exceptions import WebDriverException, SessionNotCreatedException
import platform
import subprocess
import shutil
import random
from tenacity import retry, stop_after_attempt, wait_exponential
import csv
import nltk
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

class PartnershipScraper:
    def __init__(self):
        self.companies = [
            ("SAP", [
                "https://news.sap.com/",
                "https://news.sap.com/topics/business-technology-platform/",
                "https://news.sap.com/topics/partnerships/",
                "https://www.sap.com/about/company/innovation.html"
            ]),
            ("Salesforce", [
                "https://www.salesforce.com/news/",
                "https://www.salesforce.com/company/news-press/press-releases/",
                "https://www.salesforce.com/blog/category/integration/"
            ]),
            ("Snowflake", [
                "https://www.snowflake.com/news-and-events/",
                "https://www.snowflake.com/blog/",
                "https://investors.snowflake.com/news/",
                "https://www.snowflake.com/blog/category/partners/"
            ]),
            ("Databricks", [
                "https://www.databricks.com/blog",
                "https://www.databricks.com/company/newsroom",
                "https://www.databricks.com/blog/category/engineering",
                "https://www.databricks.com/blog/category/product"
            ]),
            ("Microsoft", [
                "https://news.microsoft.com/",
                "https://azure.microsoft.com/en-us/blog/",
                "https://techcommunity.microsoft.com/t5/azure-data-blog/bg-p/AzureDataBlog"
            ]),
            ("Oracle", [
                "https://www.oracle.com/news/",
                "https://www.oracle.com/news/announcement/",
                "https://blogs.oracle.com/cloud-infrastructure/"
            ]),
            ("IBM", [
                "https://newsroom.ibm.com/",
                "https://www.ibm.com/blog/",
                "https://www.ibm.com/cloud/blog/"
            ]),
            ("Teradata", [
                "https://www.teradata.com/Press-Releases",
                "https://www.teradata.com/Blogs",
                "https://www.teradata.com/About-Us/Newsroom"
            ]),
            ("Cloudera", [
                "https://www.cloudera.com/about/news-and-blogs.html",
                "https://blog.cloudera.com/",
                "https://www.cloudera.com/about/news-and-press.html"
            ]),
            ("MongoDB", [
                "https://www.mongodb.com/newsroom",
                "https://www.mongodb.com/blog",
                "https://www.mongodb.com/blog/channel/company"
            ])
        ]
        
        self.search_terms = [
            "partnership", "integration", "data sharing",
            "data platform", "cloud platform", "data lake",
            "data warehouse", "collaboration", "joint solution",
            "strategic alliance"
        ]
        
        self.results_file = "partnership_articles.json"
        self.csv_file = "partnership_articles.csv"
        self.setup_logging()
        self.results = self.load_existing_results()
        self.min_date = datetime(2021, 1, 1)
        self.processed_urls = set()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("scraper.log"),
                logging.StreamHandler()
            ]
        )

    def load_existing_results(self) -> List[Dict]:
        if os.path.exists(self.results_file):
            try:
                with open(self.results_file, 'r') as f:
                    results = json.load(f)
                    # Initialize processed_urls with existing URLs
                    self.processed_urls = {article['url'] for article in results}
                    logging.info(f"Loaded {len(results)} existing results")
                    return results
            except json.JSONDecodeError:
                logging.warning(f"Could not load {self.results_file}, starting with empty results")
        logging.info("Loaded 0 existing results")
        return []

    def save_results(self):
        # Save to JSON
        with open(self.results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        # Save to CSV
        if self.results:
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.results[0].keys())
                writer.writeheader()
                writer.writerows(self.results)
        
        logging.info(f"Saved {len(self.results)} articles to {self.results_file} and {self.csv_file}")

    def setup_driver(self) -> webdriver.Chrome:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        logging.info("WebDriver setup successful")
        return driver

    def normalize_url(self, base_url: str, url: str) -> str:
        if not url:
            return ""
        if url.startswith("//"):
            return f"https:{url}"
        return urljoin(base_url, url)

    def is_valid_url(self, url: str) -> bool:
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def scroll_page(self, driver: webdriver.Chrome):
        try:
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception as e:
            logging.error(f"Error scrolling page: {str(e)}")
            raise

    def is_article_duplicate(self, url: str) -> bool:
        """Check if an article with the given URL already exists in results."""
        return url in self.processed_urls or any(article['url'] == url for article in self.results)

    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats and return a datetime object."""
        try:
            # Common date formats
            formats = [
                "%Y-%m-%d",
                "%d.%m.%Y",
                "%d/%m/%Y",
                "%B %d, %Y",
                "%b %d, %Y",
                "%Y/%m/%d"
            ]
            
            # Try each format
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt)
                except ValueError:
                    continue
            
            # If no format matches, try to extract date using regex
            date_pattern = r'\d{4}-\d{2}-\d{2}|\d{2}[-/]\d{2}[-/]\d{4}'
            match = re.search(date_pattern, date_str)
            if match:
                date_str = match.group()
                for fmt in formats:
                    try:
                        return datetime.strptime(date_str, fmt)
                    except ValueError:
                        continue
            
            return None
        except Exception as e:
            logging.error(f"Error parsing date '{date_str}': {str(e)}")
            return None

    def is_valid_date(self, date_str: str) -> bool:
        """Check if the article date is from 2021 or later."""
        parsed_date = self.parse_date(date_str)
        if not parsed_date:
            return False
        return parsed_date >= self.min_date

    def extract_article_data(self, driver: webdriver.Chrome, url: str, company: str) -> Optional[Dict]:
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Try different methods to find the article title
            title = None
            for selector in ['h1', 'h2', '.article-title', '.post-title']:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            if not title:
                return None
            
            # Try different methods to find the article text
            text = ""
            for selector in ['article', '.article-content', '.post-content', '.entry-content']:
                content = soup.select_one(selector)
                if content:
                    text = content.get_text(strip=True)
                    break
            
            if not text:
                paragraphs = soup.find_all('p')
                text = ' '.join(p.get_text(strip=True) for p in paragraphs)
            
            # Check if any search terms are present in title or text
            matched_terms = []
            for term in self.search_terms:
                if term.lower() in title.lower() or term.lower() in text.lower():
                    matched_terms.append(term)
            
            if not matched_terms:
                return None
            
            logging.info(f"Found matching terms in text: {matched_terms}")
            
            # Try to find the date
            date = None
            for selector in ['time', '.date', '.article-date', '.post-date']:
                date_elem = soup.select_one(selector)
                if date_elem:
                    date = date_elem.get_text(strip=True)
                    break
            
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")
            
            # Validate the date
            if not self.is_valid_date(date):
                logging.info(f"Skipping article from {date} (before 2021)")
                return None
            
            return {
                "company": company,
                "title": title,
                "url": url,
                "date": date,
                "matched_terms": matched_terms,
                "text": text[:500] + "..."  # Store only first 500 characters
            }
            
        except Exception as e:
            logging.error(f"Error extracting article data from {url}: {str(e)}")
            return None

    def search_company_news(self, company: str, urls: List[str]):
        driver = None
        try:
            driver = self.setup_driver()
            for url in urls:
                logging.info(f"Scraping {company} - {url}")
                try:
                    driver.get(url)
                    self.scroll_page(driver)
                    
                    # Find all article links
                    links = []
                    for tag in ['a', 'h2', 'h3']:
                        elements = driver.find_elements(By.TAG_NAME, tag)
                        for elem in elements:
                            href = elem.get_attribute('href')
                            if href and self.is_valid_url(href):
                                links.append(href)
                    
                    logging.info(f"Found {len(links)} potential articles on {url}")
                    
                    # Process each article
                    for link in links:
                        # Skip if we've already processed this URL
                        if self.is_article_duplicate(link):
                            logging.info(f"Skipping duplicate article: {link}")
                            continue
                            
                        try:
                            driver.get(link)
                            article_data = self.extract_article_data(driver, link, company)
                            if article_data:
                                self.results.append(article_data)
                                self.processed_urls.add(link)
                                self.save_results()  # Save after each successful article
                        except Exception as e:
                            logging.error(f"Error processing article {link}: {str(e)}")
                            continue
                            
                except Exception as e:
                    logging.error(f"Error scraping {company} - {url}: {str(e)}")
                    continue
                    
        except Exception as e:
            logging.error(f"Error in search_company_news for {company}: {str(e)}")
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def run(self):
        with tqdm(total=len(self.companies), desc="Scraping companies") as pbar:
            for company, urls in self.companies:
                logging.info(f"\nScraping {company}...")
                self.search_company_news(company, urls)
                self.save_results()
                pbar.update(1)

if __name__ == "__main__":
    scraper = PartnershipScraper()
    scraper.run() 
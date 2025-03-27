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
            ]),
            ("Collibra", [
                "https://www.collibra.com/news",
                "https://www.collibra.com/blog",
                "https://www.collibra.com/press-releases"
            ]),
            ("Confluent", [
                "https://www.confluent.io/blog/",
                "https://www.confluent.io/news/",
                "https://www.confluent.io/press-releases/"
            ]),
            ("DataRobot", [
                "https://www.datarobot.com/news/",
                "https://www.datarobot.com/blog/",
                "https://www.datarobot.com/press-releases/"
            ]),
            ("Google Cloud", [
                "https://cloud.google.com/blog",
                "https://cloud.google.com/news",
                "https://cloud.google.com/blog/products/ai-machine-learning"
            ]),
            ("Palantir", [
                "https://www.palantir.com/news/",
                "https://www.palantir.com/blog/",
                "https://investors.palantir.com/news-releases"
            ]),
            ("Informatica", [
                "https://www.informatica.com/news.html",
                "https://www.informatica.com/blogs.html",
                "https://www.informatica.com/about-us/newsroom.html"
            ]),
            ("ServiceNow", [
                "https://www.servicenow.com/newsroom.html",
                "https://www.servicenow.com/blog.html",
                "https://www.servicenow.com/community/tech-tips-and-tricks.html"
            ]),
            ("NASDAQ", [
                "https://www.nasdaq.com/news-and-insights/tech",
                "https://www.nasdaq.com/news-and-insights/company-news",
                "https://www.nasdaq.com/news-and-insights/market-movers"
            ]),
            ("NYSE", [
                "https://www.nyse.com/news-events",
                "https://www.nyse.com/technology",
                "https://www.nyse.com/ipo-center/news"
            ]),
            ("DAX", [
                "https://www.deutsche-boerse.com/dbg-en/news-views",
                "https://www.deutsche-boerse.com/dbg-en/technology",
                "https://www.deutsche-boerse.com/dbg-en/company-news"
            ]),
            ("Reuters", [
                "https://www.reuters.com/technology/",
                "https://www.reuters.com/markets/",
                "https://www.reuters.com/companies/"
            ]),
            ("Bloomberg", [
                "https://www.bloomberg.com/technology",
                "https://www.bloomberg.com/markets",
                "https://www.bloomberg.com/companies"
            ]),
            ("Financial Times", [
                "https://www.ft.com/technology",
                "https://www.ft.com/markets",
                "https://www.ft.com/companies"
            ])
        ]
        
        self.search_terms = [
            "partnership", "integration", "data sharing",
            "data platform", "cloud platform", "data lake",
            "data warehouse", "collaboration", "joint solution",
            "strategic alliance", "SAP BDC", "Business Data Cloud",
            "SAP Databricks"
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

    def save_webpage_content(self, url: str, company: str) -> str:
        """Save webpage content as UTF-16 LE encoded text file.
        
        Args:
            url: The URL of the webpage
            company: The company name for the filename
            
        Returns:
            The path to the saved file
        """
        try:
            # Create a safe filename from the URL
            safe_url = url.replace('https://', '').replace('http://', '').replace('/', '_')
            filename = f"webpage_content/{company}_{safe_url}.txt"
            
            # Skip if file already exists
            if os.path.exists(filename):
                logging.info(f"File already exists, skipping: {filename}")
                return filename
            
            # Ensure the directory exists
            os.makedirs("webpage_content", exist_ok=True)
            
            # First try with requests
            try:
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                response.raise_for_status()
                html_content = response.text
            except (requests.Timeout, requests.RequestException) as e:
                logging.info(f"Requests failed for {url}, trying with Selenium: {str(e)}")
                # If requests fails, try with Selenium
                driver = self.setup_driver()
                try:
                    driver.get(url)
                    # Wait for the page to load
                    time.sleep(5)  # Give time for Cloudflare to process
                    html_content = driver.page_source
                finally:
                    driver.quit()
            
            # Parse with BeautifulSoup to get clean text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text content
            text = soup.get_text()
            
            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            # Save as UTF-16 LE
            with open(filename, 'w', encoding='utf-16le') as f:
                f.write(text)
            
            logging.info(f"Saved webpage content to {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Error saving webpage content from {url}: {str(e)}")
            return None

    def save_all_processed_webpages(self):
        """Save content of all processed URLs as UTF-16 LE encoded text files."""
        try:
            for article in self.results:
                url = article['url']
                company = article['company']
                self.save_webpage_content(url, company)
        except Exception as e:
            logging.error(f"Error saving processed webpages: {str(e)}")

    def search_company_news(self, company: str, urls: List[str]):
        driver = None
        try:
            driver = self.setup_driver()
            for url in urls:
                logging.info(f"Scraping {company} - {url}")
                try:
                    # Save the webpage content
                    self.save_webpage_content(url, company)
                    
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
                            # Save the article content
                            self.save_webpage_content(link, company)
                            
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

    def filter_companies(self, company_names: List[str]) -> List[tuple]:
        """Filter companies by their names.
        
        Args:
            company_names: List of company names to include in the search
            
        Returns:
            List of tuples containing (company_name, urls) for filtered companies
        """
        filtered_companies = []
        for company, urls in self.companies:
            if company in company_names:
                filtered_companies.append((company, urls))
                logging.info(f"Included company: {company}")
            else:
                logging.info(f"Excluded company: {company}")
        return filtered_companies

    def run(self, selected_companies: Optional[List[str]] = None):
        """Run the scraper for all companies or selected companies.
        
        Args:
            selected_companies: Optional list of company names to scrape. If None, all companies will be scraped.
        """
        companies_to_scrape = self.filter_companies(selected_companies) if selected_companies else self.companies
        
        with tqdm(total=len(companies_to_scrape), desc="Scraping companies") as pbar:
            for company, urls in companies_to_scrape:
                logging.info(f"\nScraping {company}...")
                self.search_company_news(company, urls)
                self.save_results()
                pbar.update(1)

    def save_processed_urls_to_files(self):
        """Save all processed URLs to text files, organized by company."""
        try:
            # Create a directory for URL files if it doesn't exist
            os.makedirs("url_files", exist_ok=True)
            
            # Group URLs by company
            company_urls = {}
            for article in self.results:
                company = article['company']
                if company not in company_urls:
                    company_urls[company] = []
                company_urls[company].append(article['url'])
            
            # Save URLs for each company
            for company, urls in company_urls.items():
                # Create a safe filename
                safe_company = company.replace(' ', '_').replace('/', '_')
                filename = f"url_files/{safe_company}_urls.txt"
                
                # Save URLs as UTF-16 LE
                with open(filename, 'w', encoding='utf-16le') as f:
                    f.write(f"URLs for {company}:\n\n")
                    for url in urls:
                        f.write(f"{url}\n")
                
                logging.info(f"Saved {len(urls)} URLs for {company} to {filename}")
            
            # Save all URLs to a single file
            all_urls_file = "url_files/all_processed_urls.txt"
            with open(all_urls_file, 'w', encoding='utf-16le') as f:
                f.write("All Processed URLs:\n\n")
                for company, urls in company_urls.items():
                    f.write(f"\n=== {company} ===\n")
                    for url in urls:
                        f.write(f"{url}\n")
            
            logging.info(f"Saved all URLs to {all_urls_file}")
            
        except Exception as e:
            logging.error(f"Error saving URLs to files: {str(e)}")

if __name__ == "__main__":
    scraper = PartnershipScraper()
    
    # First, save content of any existing processed URLs
    scraper.save_all_processed_webpages()
    
    # Then run scraper for newly added companies and organizations
    new_companies = [
        "Collibra", "Confluent", "DataRobot", "Google Cloud", 
        "Palantir", "Informatica", "ServiceNow", "NASDAQ", 
        "NYSE", "DAX", "Reuters", "Bloomberg", "Financial Times"
    ]
    scraper.run(selected_companies=new_companies)
    
    # After scraping, save content of all processed URLs
    scraper.save_all_processed_webpages() 
import os
import json
import pandas as pd
from typing import List, Dict, Set
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.chunk import ne_chunk
from collections import Counter
from datetime import datetime
import logging

# Download required NLTK data
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('content_analysis.log'),
        logging.StreamHandler()
    ]
)

class ContentAnalyzer:
    def __init__(self):
        self.themes = {
            'type_of_partnership': {
                'technology_partnership': {
                    'keywords': ['technical integration', 'joint development', 'seamless integration', 'zero-copy integration'],
                    'description': 'Companies cooperate primarily for the integration of technologies, data exchange, or API interfaces.'
                },
                'market_partnership': {
                    'keywords': ['joint sales', 'co-marketing', 'market launch', 'go-to-market', 'market strategy'],
                    'description': 'Companies collaborate to develop joint market strategies or go-to-market initiatives.'
                },
                'strategic_alliance': {
                    'keywords': ['long-term cooperation', 'strategic significance', 'capital participation', 'long-term investment'],
                    'description': 'Long-term collaborations with strategic significance, often involving capital participation or long-term investments.'
                }
            },
            'exchange_processes': {
                'data_integration': {
                    'keywords': ['zero-copy', 'data platform'],
                    'description': 'Partner companies enable data exchange without physically copying the data.'
                },
                'knowledge_transfer': {
                    'keywords': ['know-how', 'best practices', 'research findings', 'training', 'educational initiatives'],
                    'description': 'Partners exchange know-how, best practices, or research findings.'
                },
                'co_branding_marketing': {
                    'keywords': ['joint advertising', 'co-marketing', 'co-branding', 'joint promotion'],
                    'description': 'Companies appear together in external communication to promote their products or solutions.'
                }
            },
            'strategic_positioning': {
                'dominant_actor': {
                    'keywords': ['lead in partnership', 'dictate direction', 'global network', 'primary benefit'],
                    'description': 'One company takes the lead in the partnership or dictates the direction.'
                },
                'equal_cooperation': {
                    'keywords': ['equal footing', 'mutual trust', 'equal collaboration', 'shared responsibility'],
                    'description': 'Both companies operate on equal footing and share responsibility.'
                },
                'asymmetric_cooperation': {
                    'keywords': ['unequal resource', 'more responsibility', 'benefit more', 'primary advantage'],
                    'description': 'One company benefits more than the other, or one party assumes more responsibility.'
                }
            },
            'narratives': {
                'innovation_focus': {
                    'keywords': ['innovation', 'new technologies', 'digital transformation', 'progress', 'new standards'],
                    'description': 'The partnership is portrayed as a driver of innovation.'
                },
                'efficiency_promise': {
                    'keywords': ['faster', 'more efficient', 'optimized', 'productivity', 'efficiency'],
                    'description': 'The collaboration is justified with increased efficiency or productivity.'
                },
                'market_dominance': {
                    'keywords': ['leading', 'game-changer', 'market standard', 'revolutionize', 'market-changing'],
                    'description': 'The partnership is described as market-changing or revolutionary.'
                }
            }
        }
        
        self.stop_words = set(stopwords.words('english'))
        
    def read_text_file(self, file_path: str) -> str:
        """Read a UTF-16 LE encoded text file."""
        try:
            with open(file_path, 'r', encoding='utf-16le') as f:
                return f.read()
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return ""

    def extract_named_entities(self, text: str) -> List[tuple]:
        """Extract named entities using NLTK."""
        tokens = word_tokenize(text)
        pos_tags = pos_tag(tokens)
        named_entities = ne_chunk(pos_tags)
        
        entities = []
        for chunk in named_entities:
            if hasattr(chunk, 'label'):
                entities.append((chunk.leaves()[0][0], chunk.label()))
        
        return entities

    def extract_key_phrases(self, text: str) -> List[str]:
        """Extract key phrases using NLTK's part-of-speech tagging."""
        tokens = word_tokenize(text)
        pos_tags = pos_tag(tokens)
        
        # Extract noun phrases (sequences of nouns)
        key_phrases = []
        current_phrase = []
        
        for word, tag in pos_tags:
            if tag.startswith('NN'):  # Noun
                current_phrase.append(word)
            elif current_phrase:
                key_phrases.append(' '.join(current_phrase))
                current_phrase = []
        
        if current_phrase:
            key_phrases.append(' '.join(current_phrase))
        
        return key_phrases

    def analyze_text(self, text: str) -> Dict:
        """Analyze text content for themes and key information."""
        # Extract sentences
        sentences = sent_tokenize(text)
        
        # Extract named entities
        entities = self.extract_named_entities(text)
        
        # Extract key phrases
        key_phrases = self.extract_key_phrases(text)
        
        # Analyze themes
        theme_matches = {}
        for category, subthemes in self.themes.items():
            theme_matches[category] = {}
            for subtheme, details in subthemes.items():
                matches = []
                for sentence in sentences:
                    if any(keyword.lower() in sentence.lower() for keyword in details['keywords']):
                        matches.append({
                            'sentence': sentence,
                            'matched_keywords': [k for k in details['keywords'] if k.lower() in sentence.lower()],
                            'description': details['description']
                        })
                theme_matches[category][subtheme] = matches
        
        # Extract important terms (excluding stop words)
        words = word_tokenize(text.lower())
        words = [word for word in words if word.isalnum() and word not in self.stop_words]
        term_frequency = Counter(words)
        
        return {
            'num_sentences': len(sentences),
            'num_words': len(words),
            'theme_matches': theme_matches,
            'key_phrases': key_phrases[:10],  # Top 10 key phrases
            'entities': entities,
            'term_frequency': dict(term_frequency.most_common(20))  # Top 20 terms
        }

    def analyze_directory(self, directory: str) -> List[Dict]:
        """Analyze all text files in a directory."""
        results = []
        
        # Load existing results if available
        existing_results = []
        existing_files = set()
        try:
            with open('content_analysis_results_20250327_115020.json', 'r', encoding='utf-8') as f:
                existing_results = json.load(f)
                existing_files = {r['filename'] for r in existing_results}
        except FileNotFoundError:
            pass
        
        for filename in os.listdir(directory):
            if filename.endswith('.txt'):
                # Skip if file was already analyzed
                if filename in existing_files:
                    logging.info(f"Skipping already analyzed file: {filename}")
                    continue
                    
                file_path = os.path.join(directory, filename)
                logging.info(f"Analyzing {filename}")
                
                # Read and analyze content
                content = self.read_text_file(file_path)
                if content:
                    analysis = self.analyze_text(content)
                    analysis['filename'] = filename
                    results.append(analysis)
        
        # Combine new results with existing ones
        return existing_results + results

    def generate_summary(self, results: List[Dict]) -> Dict:
        """Generate a summary of the analysis results."""
        summary = {
            'total_files': len(results),
            'total_sentences': sum(r['num_sentences'] for r in results),
            'total_words': sum(r['num_words'] for r in results),
            'theme_statistics': {},
            'common_entities': Counter(),
            'common_terms': Counter()
        }
        
        # Aggregate theme matches
        for category, subthemes in self.themes.items():
            summary['theme_statistics'][category] = {}
            for subtheme, details in subthemes.items():
                theme_matches = []
                for result in results:
                    theme_matches.extend(result['theme_matches'][category][subtheme])
                summary['theme_statistics'][category][subtheme] = {
                    'num_matches': len(theme_matches),
                    'description': details['description'],
                    'examples': [m['sentence'] for m in theme_matches[:5]]  # Top 5 examples
                }
        
        # Aggregate entities and terms
        for result in results:
            # Convert entity tuples to strings before updating Counter
            entity_strings = [f"{entity[0]}_{entity[1]}" for entity in result['entities']]
            summary['common_entities'].update(entity_strings)
            summary['common_terms'].update(result['term_frequency'])
        
        return summary

    def save_results(self, results: List[Dict], summary: Dict):
        """Save analysis results to JSON files."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save detailed results
        results_file = f'content_analysis_results_{timestamp}.json'
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        # Save summary
        summary_file = f'content_analysis_summary_{timestamp}.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Saved results to {results_file} and {summary_file}")

def main():
    analyzer = ContentAnalyzer()
    
    # Analyze webpage content
    logging.info("Starting content analysis...")
    results = analyzer.analyze_directory('webpage_content')
    
    # Generate summary
    logging.info("Generating summary...")
    summary = analyzer.generate_summary(results)
    
    # Save results
    logging.info("Saving results...")
    analyzer.save_results(results, summary)
    
    logging.info("Content analysis completed!")

if __name__ == "__main__":
    main() 
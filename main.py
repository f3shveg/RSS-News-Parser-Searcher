import sqlite3
import threading
from datetime import datetime, timedelta
import spacy
from newspaper import Article
from collections import defaultdict
from contextlib import contextmanager
import re
import feedparser
import schedule
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import logging
import os
import json
from pathlib import Path
import hashlib

nlp = spacy.load("ru_core_news_lg")

class ArticleStorage:
    def __init__(self, base_dir="articles"):
        """Initialize storage with base directory for articles"""
        self.base_dir = Path(base_dir)
        self._create_directory_structure()
        
    def _create_directory_structure(self):
        """Create necessary directories if they don't exist"""
        # Create main articles directory
        self.base_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for better organization
        (self.base_dir / "metadata").mkdir(exist_ok=True)
        (self.base_dir / "content").mkdir(exist_ok=True)
        
        # Create index files if they don't exist
        if not (self.base_dir / "metadata" / "entity_index.json").exists():
            self._save_json({}, "metadata/entity_index.json")
        if not (self.base_dir / "metadata" / "url_index.json").exists():
            self._save_json({}, "metadata/url_index.json")

    def _generate_filename(self, url):
        """Generate a unique filename based on URL and timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        url_hash = hashlib.md5(url.encode()).hexdigest()[:10]
        return f"{timestamp}_{url_hash}"

    def _save_json(self, data, relative_path):
        """Save JSON data to a file"""
        with open(self.base_dir / relative_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_json(self, relative_path):
        """Load JSON data from a file"""
        try:
            with open(self.base_dir / relative_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def store_article(self, url):
        """Download and store article from URL"""
        try:
            # Check if URL already exists
            url_index = self._load_json("metadata/url_index.json")
            if url in url_index:
                print(f"Article already exists: {url}")
                return False

            # Download and parse article
            article = Article(url)
            article.download()
            article.parse()

            # Generate unique filename
            filename = self._generate_filename(url)
            
            # Store article content
            content_path = self.base_dir / "content" / f"{filename}.txt"
            with open(content_path, 'w', encoding='utf-8') as f:
                f.write(f"Title: {article.title}\n")
                f.write(f"URL: {url}\n")
                f.write(f"Published: {article.publish_date}\n")
                f.write("\n" + "="*50 + "\n\n")
                f.write(article.text)

            # Store metadata
            metadata = {
                'title': article.title,
                'publish_date': str(article.publish_date),
                'url': url,
                'filename': f"{filename}.txt",
                'entities': {},
                'actions': []
            }

            # Process entities and actions
            doc = nlp(article.text)
            for ent in doc.ents:
                if ent.label_ in ["LOC", "PER", "ORG"]:
                    normalized = self._normalize_entity(ent.text, ent.label_)
                    metadata['entities'][normalized] = ent.label_

            # Store actions for persons
            for ent in doc.ents:
                if ent.label_ == "PER":
                    if ent.root.dep_ in ('nsubj', 'nsubjpass') and ent.root.head.pos_ == 'VERB':
                        metadata['actions'].append({
                            'person': self._normalize_entity(ent.text, 'PER'),
                            'verb': ent.root.head.lemma_.lower()
                        })

            # Update indices
            self._update_indices(filename, url, metadata)
            
            return True

        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
            return False

    def _update_indices(self, filename, url, metadata):
        """Update the index files with new article information"""
        # Make sure filename doesn't include .txt extension for the indices
        base_filename = filename.replace('.txt', '')
        
        # Update URL index
        url_index = self._load_json("metadata/url_index.json")
        url_index[url] = base_filename
        self._save_json(url_index, "metadata/url_index.json")

        # Update entity index
        entity_index = self._load_json("metadata/entity_index.json")
        for entity, entity_type in metadata['entities'].items():
            if entity not in entity_index:
                entity_index[entity] = {'type': entity_type, 'articles': []}
            if base_filename not in entity_index[entity]['articles']:
                entity_index[entity]['articles'].append(base_filename)
        self._save_json(entity_index, "metadata/entity_index.json")

        # Save article metadata
        self._save_json(metadata, f"metadata/{base_filename}.json")

    def _normalize_entity(self, text, entity_type):
        """Normalize entity names"""
        doc = nlp(text.lower())
        
        if entity_type == "LOC":
            location_map = {
                r"\bмоскв[а-я]*\b": "москва",
                r"\b(mosk|msk|mosc)\w*\b": "москва",
                r"\bмск\b": "москва"
            }
            
            text = " ".join([token.lemma_ for token in doc])
            for pattern, base in location_map.items():
                if re.search(pattern, text):
                    return base
            return text
        
        elif entity_type == "PER":
            return " ".join([token.lemma_.title() for token in doc[-1:]])
        
        return " ".join([token.lemma_.lower() for token in doc])

    def search_articles(self, search_term, entity_type=None):
        """Search for articles containing specific entities"""
        entity_index = self._load_json("metadata/entity_index.json")
        results = []

        try:
            normalized_term = self._normalize_entity(search_term, entity_type)
            print(f"Searching for normalized term: {normalized_term}")  # Debug output
            
            if normalized_term in entity_index:
                print(f"Found term in index with {len(entity_index[normalized_term]['articles'])} articles")  # Debug output
                
                if not entity_type or entity_index[normalized_term]['type'] == entity_type:
                    for base_filename in entity_index[normalized_term]['articles']:
                        try:
                            # Load metadata
                            metadata_path = self.base_dir / "metadata" / f"{base_filename}.json"
                            if not metadata_path.exists():
                                print(f"Warning: Metadata file not found: {metadata_path}")
                                continue
                                
                            metadata = self._load_json(f"metadata/{base_filename}.json")
                            
                            # Load content
                            content_path = self.base_dir / "content" / f"{base_filename}.txt"
                            if not content_path.exists():
                                print(f"Warning: Content file not found: {content_path}")
                                continue
                                
                            with open(content_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                                
                            results.append({
                                'title': metadata['title'],
                                'date': metadata['publish_date'],
                                'url': metadata['url'],
                                'content_preview': content[:200] + "..."
                            })
                            
                        except Exception as e:
                            print(f"Error processing {base_filename}: {str(e)}")
                            continue
            else:
                print(f"Term '{normalized_term}' not found in index")  # Debug output

        except Exception as e:
            print(f"Search error: {str(e)}")
            
        return results

    def _debug_print_indices(self):
        """Debug method to print current indices"""
        print("\nEntity Index:")
        entity_index = self._load_json("metadata/entity_index.json")
        print(json.dumps(entity_index, indent=2, ensure_ascii=False))
        
        print("\nURL Index:")
        url_index = self._load_json("metadata/url_index.json")
        print(json.dumps(url_index, indent=2, ensure_ascii=False))

class FeedMonitor:
    def __init__(self, storage):
        self.storage = storage
        self.feeds_file = Path("feeds.json")
        self.feeds = self._load_feeds()
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='feed_monitor.log'
        )
        self.logger = logging.getLogger(__name__)

    def _load_feeds(self):
        """Load saved RSS feeds from file"""
        try:
            if self.feeds_file.exists():
                with open(self.feeds_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"Error loading feeds: {e}")
            return {}

    def _save_feeds(self):
        """Save RSS feeds to file"""
        try:
            with open(self.feeds_file, 'w', encoding='utf-8') as f:
                json.dump(self.feeds, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"Error saving feeds: {e}")

    def add_feed(self, url, interval=5):
        """Add a new feed with proper error handling"""
        try:
            # First validate the feed URL
            feed = feedparser.parse(url)
            if feed.get('bozo', 1) == 1:
                raise ValueError("Invalid RSS feed URL")
            
            # Load existing feeds
            feeds = {}
            if self.feeds_file.exists():
                try:
                    with open(self.feeds_file, 'r', encoding='utf-8') as f:
                        feeds = json.load(f)
                except json.JSONDecodeError:
                    # If JSON file is corrupted, start fresh
                    feeds = {}
            
            # Add new feed
            feeds[url] = {
                'last_check': str(datetime.now() - timedelta(minutes=interval)),
                'interval': interval,
                'title': feed.feed.get('title', 'Unknown'),
                'description': feed.feed.get('description', '')
            }
            
            # Save feeds with proper encoding
            self._save_feeds()
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error adding feed: {str(e)}")
            return False

    def remove_feed(self, feed_url):
        """Remove a feed from monitoring"""
        if feed_url in self.feeds:
            del self.feeds[feed_url]
            self._save_feeds()
            self.logger.info(f"Removed feed: {feed_url}")
            return True
        return False

    def check_feed(self, feed_url):
        """Check a single feed for new articles"""
        try:
            feed = feedparser.parse(feed_url)
            last_check = datetime.fromisoformat(self.feeds[feed_url]['last_check'])
            current_time = datetime.now()
            
            new_articles = []
            for entry in feed.entries:
                # Get article publish time
                if hasattr(entry, 'published_parsed'):
                    pub_date = datetime(*entry.published_parsed[:6])
                else:
                    pub_date = current_time  # Use current time if no publish date

                # If article is newer than last check, process it
                if pub_date > last_check:
                    if hasattr(entry, 'link'):
                        new_articles.append(entry.link)

            # Update last check time
            self.feeds[feed_url]['last_check'] = str(current_time)
            self._save_feeds()

            # Process new articles
            for url in new_articles:
                try:
                    self.storage.store_article(url)
                    self.logger.info(f"Processed new article: {url}")
                except Exception as e:
                    self.logger.error(f"Error processing article {url}: {e}")

        except Exception as e:
            self.logger.error(f"Error checking feed {feed_url}: {e}")

    def start_monitoring(self):
        """Start the monitoring process"""
        self.running = True
        
        def run_pending():
            while self.running:
                current_time = datetime.now()
                
                # Check each feed
                for feed_url, feed_info in self.feeds.items():
                    try:
                        last_check = datetime.fromisoformat(feed_info['last_check'])
                        if current_time - last_check >= timedelta(minutes=feed_info['interval']):
                            self.check_feed(feed_url)
                    except Exception as e:
                        self.logger.error(f"Error in feed check loop: {e}")
                
                time.sleep(60)  # Wait 1 minute before next check
        
        self.executor.submit(run_pending)
        self.logger.info("Feed monitoring started")

    def stop_monitoring(self):
        """Stop the monitoring process"""
        self.running = False
        self.executor.shutdown(wait=True)
        self.logger.info("Feed monitoring stopped")

    def list_feeds(self):
        """Return list of monitored feeds"""
        return [
            {
                'url': url,
                'title': info['title'],
                'description': info['description'],
                'interval': info['interval'],
                'last_check': info['last_check']
            }
            for url, info in self.feeds.items()
        ]

if __name__ == "__main__":
    storage = ArticleStorage("articles")
    
    print("News Article Processing System")
    print("-----------------------------")
    print("Note: RSS monitoring is handled by background service")
    
    while True:
        print("\nOptions:")
        print("1. Add new article from URL")
        print("2. Search articles")
        print("3. Add RSS feed to monitor")
        print("4. List monitored feeds")
        print("5. Remove RSS feed")
        print("6. Exit")
        
        choice = input("Enter your choice (1-6): ")

        if choice == '1':
            url = input("\nEnter article URL: ").strip()
            if not url.startswith('http'):
                print("Invalid URL format. Please include http:// or https://")
                continue
                
            if storage.store_article(url):
                print("\nArticle processed successfully!")
            else:
                print("\nFailed to process article")

        elif choice == '2':
            search_term = input("\nEnter search term: ").strip()
            entity_type = input("Search type (LOC/PER/ORG): ").strip().upper()
            
            if entity_type not in ['LOC', 'PER', 'ORG']:
                print("Invalid entity type. Using LOC as default.")
                entity_type = 'LOC'
            
            print("\nSearching articles...")
            # Add debug output
            storage._debug_print_indices()
            
            results = storage.search_articles(search_term, entity_type)
            
            print(f"\nFound {len(results)} articles:")
            for idx, article in enumerate(results, 1):
                print(f"\n{idx}. {article['title']}")
                print(f"URL: {article['url']}")
                print(f"Preview: {article['content_preview']}")

        elif choice == '3':
            url = input("\nEnter RSS feed URL: ").strip()
            try:
                interval = int(input("Enter check interval in minutes (default: 5): ") or 5)
                if add_feed(url, interval):
                    print(f"\nSuccessfully added feed: {url}")
                else:
                    print("\nFailed to add feed")
            except ValueError as e:
                print(f"\nInvalid input: {e}")

        elif choice == '4':
            feeds = {}
            if Path("feeds.json").exists():
                with open("feeds.json", 'r') as f:
                    feeds = json.load(f)
            
            if not feeds:
                print("\nNo feeds currently monitored")
            else:
                print("\nCurrently monitored feeds:")
                for feed_url, feed_info in feeds.items():
                    print(f"\nTitle: {feed_info['title']}")
                    print(f"URL: {feed_url}")
                    print(f"Check interval: {feed_info['interval']} minutes")
                    print(f"Last checked: {feed_info['last_check']}")
                    print(f"Description: {feed_info['description']}")
                    print("-" * 40)

        elif choice == '5':
            feeds = {}
            if Path("feeds.json").exists():
                with open("feeds.json", 'r') as f:
                    feeds = json.load(f)
            
            if not feeds:
                print("\nNo feeds to remove")
                continue
                
            print("\nCurrent feeds:")
            for i, feed_url in enumerate(feeds.keys(), 1):
                print(f"{i}. {feed_url}")
            
            try:
                idx = int(input("\nEnter feed number to remove: ")) - 1
                if 0 <= idx < len(feeds):
                    url = list(feeds.keys())[idx]
                    if storage.remove_feed(url):
                        print(f"\nRemoved feed: {url}")
                    else:
                        print("\nFailed to remove feed")
                else:
                    print("\nInvalid feed number")
            except ValueError:
                print("\nInvalid input")

        elif choice == '6':
            print("\nStopping monitoring and exiting system...")
            break
            
        else:
            print("Invalid choice. Please try again.")
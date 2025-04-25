import sys
import time
import logging
from pathlib import Path
import json
import feedparser
from datetime import datetime, timedelta
from newspaper import Article
import signal
import traceback

# Import your storage class
from main import ArticleStorage

class RSSMonitorDaemon:
    def __init__(self, storage=None):
        # Set up logging first
        self._setup_logging()
        
        try:
            self.logger.info("Initializing RSS Monitor Daemon...")
            # Use provided storage or create new one
            self.storage = storage if storage else ArticleStorage("articles")
            self.feeds_file = Path("feeds.json")
            self.running = True
            self.logger.info("Initialization complete")
        except Exception as e:
            self.logger.error(f"Failed to initialize daemon: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def _setup_logging(self):
        """Setup logging with detailed configuration"""
        try:
            log_file = Path('rss_monitor.log')
            logging.basicConfig(
                level=logging.DEBUG,  # Set to DEBUG for more detailed logs
                format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler(sys.stdout)  # Also log to console
                ]
            )
            self.logger = logging.getLogger(__name__)
            self.logger.info("Logging initialized")
        except Exception as e:
            print(f"Failed to setup logging: {str(e)}")
            raise

    def load_feeds(self):
        """Load saved RSS feeds from file"""
        try:
            if self.feeds_file.exists():
                with open(self.feeds_file, 'r', encoding='utf-8') as f:
                    feeds = json.load(f)
                self.logger.debug(f"Loaded {len(feeds)} feeds from {self.feeds_file}")
                return feeds
            self.logger.warning(f"Feeds file not found: {self.feeds_file}")
            return {}
        except Exception as e:
            self.logger.error(f"Error loading feeds: {str(e)}")
            self.logger.error(traceback.format_exc())
            return {}

    def check_feed(self, feed_url, feed_info):
        """Check a single feed for new articles"""
        self.logger.debug(f"Checking feed: {feed_url}")
        try:
            feed = feedparser.parse(feed_url)
            if feed.get('bozo', 0) == 1:
                self.logger.error(f"Feed parsing error for {feed_url}: {feed.get('bozo_exception')}")
                return feed_info['last_check']
                
            last_check = datetime.fromisoformat(feed_info['last_check'])
            current_time = datetime.now()
            comparison_time = current_time - timedelta(hours=24)
            
            new_articles_count = 0
            for entry in feed.entries:
                try:
                    if hasattr(entry, 'published_parsed'):
                        pub_date = datetime(*entry.published_parsed[:6])
                    else:
                        pub_date = current_time

                    if pub_date >= comparison_time and hasattr(entry, 'link'):
                        if self.storage.store_article(entry.link):
                            new_articles_count += 1
                            self.logger.info(f"Stored article: {entry.get('title', 'No title')}")
                except Exception as e:
                    self.logger.error(f"Error processing entry: {str(e)}")

            self.logger.info(f"Processed {new_articles_count} new articles from {feed_url}")
            return str(current_time)
            
        except Exception as e:
            self.logger.error(f"Error checking feed {feed_url}: {str(e)}")
            return feed_info['last_check']

    def run(self):
        """Main daemon loop"""
        self.logger.info("Starting RSS Monitor Daemon main loop")
        
        while self.running:
            try:
                feeds = self.load_feeds()
                current_time = datetime.now()
                
                for feed_url, feed_info in feeds.items():
                    if not feed_info.get('active', True):
                        continue
                        
                    try:
                        last_check = datetime.fromisoformat(feed_info['last_check'])
                        interval = timedelta(minutes=feed_info['interval'])
                        
                        if current_time - last_check >= interval:
                            new_last_check = self.check_feed(feed_url, feed_info)
                            feeds[feed_url]['last_check'] = new_last_check
                            
                            # Save updated last check time
                            with open(self.feeds_file, 'w', encoding='utf-8') as f:
                                json.dump(feeds, f, indent=2, ensure_ascii=False)
                    except Exception as e:
                        self.logger.error(f"Error processing feed {feed_url}: {str(e)}")
                        self.logger.error(traceback.format_exc())
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in main loop: {str(e)}")
                self.logger.error(traceback.format_exc())
                time.sleep(60)  # Wait before retrying

    def handle_signal(self, signum, frame):
        """Handle termination signals"""
        self.logger.info(f"Received signal {signum}")
        self.running = False

def main():
    try:
        # Create storage instance that will be shared
        storage = ArticleStorage("articles")
        
        # Pass the storage instance to the daemon
        daemon = RSSMonitorDaemon(storage)
        
        # Set up signal handlers
        signal.signal(signal.SIGTERM, daemon.handle_signal)
        signal.signal(signal.SIGINT, daemon.handle_signal)
        
        daemon.run()
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 

import sys
import time
import daemon
import logging
import signal
import lockfile
from pathlib import Path
import feedparser
from datetime import datetime, timedelta
import json
from newspaper import Article
import spacy

# Import your storage class
from main import ArticleStorage

class RSSMonitorDaemon:
    def __init__(self):
        self.storage = ArticleStorage("articles")
        self.feeds_file = Path("feeds.json")
        self.running = True
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='feed_monitor.log'
        )
        self.logger = logging.getLogger(__name__)

    def load_feeds(self):
        """Load saved RSS feeds from file"""
        try:
            if self.feeds_file.exists():
                with open(self.feeds_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"Error loading feeds: {e}")
            return {}

    def check_feed(self, feed_url, feed_info):
        """Check a single feed for new articles"""
        try:
            feed = feedparser.parse(feed_url)
            last_check = datetime.fromisoformat(feed_info['last_check'])
            current_time = datetime.now()
            
            for entry in feed.entries:
                try:
                    if hasattr(entry, 'published_parsed'):
                        pub_date = datetime(*entry.published_parsed[:6])
                    else:
                        pub_date = current_time

                    if pub_date > last_check and hasattr(entry, 'link'):
                        self.storage.store_article(entry.link)
                        self.logger.info(f"Processed new article: {entry.link}")
                except Exception as e:
                    self.logger.error(f"Error processing entry: {e}")

            return str(current_time)
        except Exception as e:
            self.logger.error(f"Error checking feed {feed_url}: {e}")
            return feed_info['last_check']

    def run(self):
        """Main daemon loop"""
        self.logger.info("RSS Monitor Daemon started")
        
        while self.running:
            try:
                feeds = self.load_feeds()
                current_time = datetime.now()
                
                for feed_url, feed_info in feeds.items():
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
                        self.logger.error(f"Error processing feed {feed_url}: {e}")
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                time.sleep(60)  # Wait before retrying

    def handle_signal(self, signum, frame):
        """Handle termination signals"""
        self.logger.info("Received termination signal")
        self.running = False

def main():
    daemon = RSSMonitorDaemon()
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, daemon.handle_signal)
    signal.signal(signal.SIGINT, daemon.handle_signal)
    
    daemon.run()

if __name__ == "__main__":
    # Create daemon context
    context = daemon.DaemonContext(
        working_directory='.',
        pidfile=lockfile.FileLock('/tmp/rss_monitor.pid'),
        files_preserve=[
            logging.getLogger().handlers[0].stream,
        ]
    )
    
    with context:
        main() 

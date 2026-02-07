"""
Auto-update scheduler for business directory
Keeps the database fresh with scheduled updates
"""

import schedule
import time
import asyncio
from business_crawler import BusinessCrawler
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_updates.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class UpdateScheduler:
    """Manages scheduled updates of business directory"""
    
    def __init__(self):
        self.crawler = BusinessCrawler()
    
    def run_update(self):
        """Run a full update cycle"""
        logger.info("Starting scheduled update...")
        start_time = datetime.now()
        
        try:
            asyncio.run(self.crawler.run_full_crawl())
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Update completed successfully in {duration:.2f} seconds")
        except Exception as e:
            logger.error(f"Update failed: {e}", exc_info=True)
    
    def start(self):
        """Start the scheduler"""
        # Schedule daily update at 2 AM
        schedule.every().day.at("02:00").do(self.run_update)
        
        # Optional: Weekly deep update on Sundays
        schedule.every().sunday.at("03:00").do(self.run_update)
        
        logger.info("Scheduler started. Updates will run daily at 2:00 AM")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")


if __name__ == "__main__":
    scheduler = UpdateScheduler()
    scheduler.start()

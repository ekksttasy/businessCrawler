# UK Business Directory Web Crawler
## Building a Google Places API Alternative

A robust, scalable web crawler for building a comprehensive UK business directory by aggregating data from multiple independent sources (NOT Google Places). Perfect for creating your own places API.

**Can be used standalone or as a Git submodule in a larger project.**

## Features

✅ **Multi-source aggregation** - Companies House, OpenStreetMap, Foursquare, Yelp, and more  
✅ **Independent of Google** - Build your own places database  
✅ **robots.txt compliance** - Respects crawling policies, especially GOV.UK  
✅ **Data deduplication** - Intelligent merging of business records from multiple sources  
✅ **Smart prioritization** - Prioritizes street-level names over parent company names  
✅ **Auto-updates** - Scheduled refreshes to keep data current  
✅ **AI descriptions** - Automatically generated business descriptions using Anthropic/OpenAI  
✅ **MySQL integration** - Works with your existing database  
✅ **Submodule ready** - Can be embedded in larger projects

## Quick Start

### Standalone Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Set up configuration
cp .env.template .env
# Edit .env and add your credentials

# Initialize and test
python init_crawler.py

# Run crawler
python business_crawler.py
```

## Required API Keys (Priority Order)

### Essential (Start Here):
1. **Companies House API** - FREE, unlimited
   - Register at [Companies House Developer Hub](https://developer.company-information.service.gov.uk/)
   - Provides official UK business registry data

2. **Anthropic API** - For AI descriptions
   - Get key at [Anthropic Console](https://console.anthropic.com/)
   - Alternative: OpenAI API

### Recommended (Generous Free Tiers):
3. **Foursquare Places API** - 100,000 FREE calls/month
   - Register at [Foursquare Developer](https://location.foursquare.com/developer/)

4. **Yelp Fusion API** - 500 FREE calls/day
   - Register at [Yelp Developers](https://www.yelp.com/developers)

### No Key Needed:
- **OpenStreetMap** - Always free via Overpass API

## Usage

### Basic Crawl

```python
import asyncio
from business_crawler import BusinessCrawler

async def main():
    crawler = BusinessCrawler()
    await crawler.run_full_crawl()

asyncio.run(main())
```

### Run from Command Line

```bash
python business_crawler.py
```

### Set Up Auto-Updates

```bash
# Run scheduler (keeps running in background)
python scheduler.py

# Or use systemd/supervisor for production
```

## Data Structure

Each business record contains:

```python
{
    "name": "Business Name Ltd",
    "address": "123 High Street, London, SW1A 1AA",
    "latitude": 51.5074,
    "longitude": -0.1278,
    "category": "Restaurant",
    "opening_hours": {
        "Monday": "09:00-17:00",
        "Tuesday": "09:00-17:00",
        ...
    },
    "average_rating": 4.5,
    "price_range": "$$",
    "ai_description": "A charming restaurant serving...",
    "source_urls": ["https://source1.com", "https://source2.com"],
    "last_updated": "2024-02-07T10:30:00"
}
```

## Ethical Crawling

This crawler:
- ✅ Respects robots.txt
- ✅ Includes delays between requests
- ✅ Uses appropriate User-Agent
- ✅ Follows rate limits
- ✅ Prioritizes official APIs over scraping

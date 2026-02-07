"""
UK Business Directory Web Crawler
Aggregates business data from multiple sources while respecting robots.txt
"""

import asyncio
import aiohttp
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict
import json
from datetime import datetime
from pathlib import Path


@dataclass
class Business:
    """Business entity with all required fields"""
    name: str
    address: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    category: Optional[str] = None
    opening_hours: Optional[Dict[str, str]] = None
    average_rating: Optional[float] = None
    price_range: Optional[str] = None  # $, $$, $$$, $$$$
    ai_description: Optional[str] = None
    source_urls: Optional[List[str]] = None
    last_updated: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


class RobotsTxtChecker:
    """Handles robots.txt compliance for all domains"""
    
    def __init__(self):
        self.parsers = {}
    
    async def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if URL can be fetched according to robots.txt"""
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        
        if robots_url not in self.parsers:
            parser = RobotFileParser()
            parser.set_url(robots_url)
            try:
                parser.read()
                self.parsers[robots_url] = parser
            except Exception as e:
                print(f"Warning: Could not read robots.txt for {parsed.netloc}: {e}")
                # Conservative approach: allow if robots.txt unavailable
                return True
        
        return self.parsers[robots_url].can_fetch(user_agent, url)


class DataAggregator:
    """Aggregates and deduplicates business data from multiple sources"""
    
    def __init__(self):
        self.businesses = {}
        self.location_index = {}  # Track businesses at same lat/lng
    
    def add_business(self, business: Business, source: str):
        """Add or merge business data"""
        key = self._generate_key(business)
        
        # Check if there's already a business at this exact location
        location_key = self._generate_location_key(business)
        if location_key and location_key in self.location_index:
            # Multiple businesses at same location
            existing_key = self.location_index[location_key]
            
            # If existing is from Companies House and new is not, prefer the new one
            if self._is_companies_house_source(self.businesses[existing_key].source_urls):
                if not self._is_companies_house_source([source]):
                    print(f"  📍 Location conflict: Replacing Companies House entry with {source} at {location_key}")
                    # Remove old entry and use new one
                    del self.businesses[existing_key]
                    business.source_urls = [source]
                    business.last_updated = datetime.now().isoformat()
                    self.businesses[key] = business
                    self.location_index[location_key] = key
                    return
        
        if key in self.businesses:
            # Merge with existing data
            self.businesses[key] = self._merge_business_data(
                self.businesses[key], 
                business,
                source
            )
        else:
            business.source_urls = [source]
            business.last_updated = datetime.now().isoformat()
            self.businesses[key] = business
            
            # Update location index
            if location_key:
                self.location_index[location_key] = key
    
    def _generate_key(self, business: Business) -> str:
        """Generate unique key for business deduplication"""
        # Use name + approximate location
        return f"{business.name.lower().strip()}_{business.address[:20].lower()}"
    
    def _generate_location_key(self, business: Business) -> str:
        """Generate location-based key for detecting co-located businesses"""
        if business.latitude and business.longitude:
            # Round to ~11 meters precision (5 decimal places)
            lat = round(business.latitude, 5)
            lng = round(business.longitude, 5)
            return f"{lat}_{lng}"
        return None
    
    def _is_companies_house_source(self, source_urls: List[str]) -> bool:
        """Check if sources include Companies House"""
        if not source_urls:
            return False
        return any('companies_house' in str(s).lower() for s in source_urls)
    
    def _merge_business_data(self, existing: Business, new: Business, source: str) -> Business:
        """
        Merge two business records, preferring more complete data
        Prioritizes non-Companies House sources for customer-facing data
        """
        merged = existing
        
        # Determine source priority
        existing_is_ch = self._is_companies_house_source(existing.source_urls)
        new_is_ch = self._is_companies_house_source([source])
        
        # RULE: For same business, prioritize non-Companies House for name/category
        # (Companies House often has formal/parent company names)
        if new_is_ch and not existing_is_ch:
            # Keep existing name/category if from better source
            pass  # Don't override with Companies House data
        elif not new_is_ch and existing_is_ch:
            # Override Companies House with better source
            if new.name:
                merged.name = new.name
            if new.category:
                merged.category = new.category
        
        # Update fields if new data is more complete
        if new.latitude and not existing.latitude:
            merged.latitude = new.latitude
            merged.longitude = new.longitude
        
        if new.category and not existing.category:
            merged.category = new.category
        
        if new.opening_hours and not existing.opening_hours:
            merged.opening_hours = new.opening_hours
        
        if new.average_rating:
            # Average ratings across sources
            if existing.average_rating:
                merged.average_rating = (existing.average_rating + new.average_rating) / 2
            else:
                merged.average_rating = new.average_rating
        
        if new.price_range and not existing.price_range:
            merged.price_range = new.price_range
        
        # Add source URL
        if merged.source_urls:
            merged.source_urls.append(source)
        else:
            merged.source_urls = [source]
        
        merged.last_updated = datetime.now().isoformat()
        
        return merged
    
    def get_all_businesses(self) -> List[Business]:
        """Return all aggregated businesses"""
        return list(self.businesses.values())


class BusinessCrawler:
    """Main crawler orchestrator"""
    
    def __init__(self, db_config: dict = None):
        self.robots_checker = RobotsTxtChecker()
        self.aggregator = DataAggregator()
        self.db_config = db_config or self._load_db_config()
        self.session = None
        self._init_database()
    
    def _load_db_config(self) -> dict:
        """Load database configuration from environment or use defaults"""
        import os
        from dotenv import load_dotenv
        
        # Support both standalone and submodule usage
        # Look for .env in current directory, then parent directories
        env_paths = [
            '.env',                    # Same directory as script
            '../.env',                 # Parent directory (if submodule)
            '../../.env',              # Two levels up (if nested submodule)
            os.path.join(os.path.dirname(__file__), '.env'),  # Script's directory
        ]
        
        for env_path in env_paths:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                print(f"📄 Loaded config from: {env_path}")
                break
        else:
            print("⚠️  No .env file found, using environment variables")
            load_dotenv()  # Still try to load from environment
        
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'business_directory'),
            'charset': 'utf8mb4'
        }
    
    def _init_database(self):
        """Initialize MySQL database and create tables if needed"""
        try:
            import mysql.connector
            from mysql.connector import Error
            
            # Connect to MySQL server
            connection = mysql.connector.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            
            if connection.is_connected():
                cursor = connection.cursor()
                
                # Create database if it doesn't exist
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                cursor.execute(f"USE {self.db_config['database']}")
                
                # Create businesses table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS businesses (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        address TEXT NOT NULL,
                        latitude DECIMAL(10, 7) NULL,
                        longitude DECIMAL(10, 7) NULL,
                        category VARCHAR(100) NULL,
                        opening_hours JSON NULL,
                        average_rating DECIMAL(3, 2) NULL,
                        price_range VARCHAR(10) NULL,
                        ai_description TEXT NULL,
                        source_urls JSON NULL,
                        last_updated DATETIME NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_business (name(100), address(100)),
                        INDEX idx_location (latitude, longitude),
                        INDEX idx_category (category),
                        INDEX idx_rating (average_rating),
                        INDEX idx_updated (last_updated)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                connection.commit()
                cursor.close()
                connection.close()
                print(f"✅ Connected to MySQL database: {self.db_config['database']}")
                
        except Error as e:
            print(f"❌ MySQL Error: {e}")
            print("Make sure MySQL is running and credentials in .env are correct")
            raise
    
    async def crawl_companies_house(self, api_key: str = None, limit: int = 1000):
        """
        Crawl Companies House API - Official UK business registry
        Free, unlimited API. Get key at: https://developer.company-information.service.gov.uk/
        """
        if not api_key:
            print("⚠️  Companies House API key required")
            print("Register at: https://developer.company-information.service.gov.uk/")
            return
        
        print("🏢 Starting Companies House crawl...")
        
        # Search for active companies
        search_url = "https://api.company-information.service.gov.uk/search/companies"
        
        # Example: Search for companies in different sectors
        search_queries = [
            "restaurant", "cafe", "hotel", "shop", "limited",
            "services", "consulting", "retail", "construction"
        ]
        
        count = 0
        for query in search_queries:
            if count >= limit:
                break
                
            params = {
                "q": query,
                "items_per_page": 100
            }
            
            try:
                async with self.session.get(
                    search_url, 
                    params=params,
                    auth=aiohttp.BasicAuth(api_key, '')  # API key as username, empty password
                ) as response:
                    if response.status != 200:
                        print(f"Error searching Companies House: {response.status}")
                        continue
                    
                    data = await response.json()
                    
                    for company in data.get('items', []):
                        if count >= limit:
                            break
                        
                        # Get detailed company info
                        company_number = company.get('company_number')
                        detail_url = f"https://api.company-information.service.gov.uk/company/{company_number}"
                        
                        async with self.session.get(
                            detail_url,
                            auth=aiohttp.BasicAuth(api_key, '')
                        ) as detail_response:
                            if detail_response.status == 200:
                                detail_data = await detail_response.json()
                                
                                # Extract address
                                address_data = detail_data.get('registered_office_address', {})
                                address = ', '.join(filter(None, [
                                    address_data.get('address_line_1'),
                                    address_data.get('address_line_2'),
                                    address_data.get('locality'),
                                    address_data.get('postal_code')
                                ]))
                                
                                # Determine category from SIC codes
                                sic_codes = detail_data.get('sic_codes', [])
                                category = self._map_sic_to_category(sic_codes[0]) if sic_codes else None
                                
                                business = Business(
                                    name=detail_data.get('company_name'),
                                    address=address,
                                    category=category
                                )
                                
                                self.aggregator.add_business(business, f"companies_house_{company_number}")
                                count += 1
                                
                                if count % 50 == 0:
                                    print(f"  Processed {count} Companies House records...")
                        
                        await asyncio.sleep(0.6)  # Rate limiting: ~100 requests/minute
                        
            except Exception as e:
                print(f"Error crawling Companies House: {e}")
        
        print(f"✅ Companies House crawl complete: {count} businesses")
    
    def _map_sic_to_category(self, sic_code: str) -> str:
        """Map SIC code to business category"""
        sic_mapping = {
            '56': 'Restaurant',
            '55': 'Hotel',
            '47': 'Retail',
            '68': 'Real Estate',
            '70': 'Consulting',
            '41': 'Construction',
            '62': 'Technology',
            '86': 'Healthcare'
        }
        # SIC codes are 5 digits, use first 2 for category
        prefix = sic_code[:2] if len(sic_code) >= 2 else sic_code
        return sic_mapping.get(prefix, 'Business')
    
    async def crawl_openstreetmap(self, bbox: str = "49.9,-7.6,58.7,1.8", limit: int = 1000):
        """
        Crawl OpenStreetMap via Overpass API
        bbox format: min_lat,min_lon,max_lat,max_lon (default is UK)
        FREE - No API key needed!
        """
        print("🗺️  Starting OpenStreetMap crawl...")
        
        overpass_url = "https://overpass-api.de/api/interpreter"
        
        # Query for various business types
        query = f"""
        [out:json][timeout:60];
        (
          node["shop"]{bbox};
          node["amenity"~"restaurant|cafe|pub|bar|bank|pharmacy"]{bbox};
          node["tourism"~"hotel|guest_house"]{bbox};
          node["office"]{bbox};
        );
        out body {limit};
        """
        
        try:
            async with self.session.post(overpass_url, data={"data": query}) as response:
                if response.status != 200:
                    print(f"Error querying OpenStreetMap: {response.status}")
                    return
                
                data = await response.json()
                count = 0
                
                for element in data.get('elements', []):
                    tags = element.get('tags', {})
                    
                    # Extract business info
                    name = tags.get('name')
                    if not name:
                        continue  # Skip unnamed POIs
                    
                    # Build address from available tags
                    address_parts = [
                        tags.get('addr:housenumber'),
                        tags.get('addr:street'),
                        tags.get('addr:city'),
                        tags.get('addr:postcode')
                    ]
                    address = ', '.join(filter(None, address_parts))
                    
                    if not address:
                        address = tags.get('addr:full', 'Address not available')
                    
                    # Determine category
                    category = (tags.get('amenity') or 
                               tags.get('shop') or 
                               tags.get('tourism') or 
                               tags.get('office') or 
                               'business')
                    
                    # Extract opening hours
                    opening_hours_str = tags.get('opening_hours')
                    opening_hours = {'raw': opening_hours_str} if opening_hours_str else None
                    
                    business = Business(
                        name=name,
                        address=address,
                        latitude=element.get('lat'),
                        longitude=element.get('lon'),
                        category=category.capitalize(),
                        opening_hours=opening_hours
                    )
                    
                    self.aggregator.add_business(business, f"osm_{element.get('id')}")
                    count += 1
                    
                    if count % 100 == 0:
                        print(f"  Processed {count} OpenStreetMap POIs...")
                
                print(f"✅ OpenStreetMap crawl complete: {count} businesses")
                
        except Exception as e:
            print(f"Error crawling OpenStreetMap: {e}")
    
    async def crawl_foursquare(self, api_key: str = None, location: str = "London,UK", limit: int = 1000):
        """
        Crawl Foursquare Places API
        100,000 free calls/month. Get key at: https://location.foursquare.com/developer/
        """
        if not api_key:
            print("⚠️  Foursquare API key required")
            print("Register at: https://location.foursquare.com/developer/")
            return
        
        print("📍 Starting Foursquare crawl...")
        
        search_url = "https://api.foursquare.com/v3/places/search"
        headers = {
            "Accept": "application/json",
            "Authorization": api_key
        }
        
        # Search in different categories
        categories = [
            "13065",  # Restaurants
            "13003",  # Bars
            "19014",  # Hotels
            "17069",  # Retail
            "12000"   # Community
        ]
        
        count = 0
        for category_id in categories:
            if count >= limit:
                break
            
            params = {
                "near": location,
                "categories": category_id,
                "limit": 50
            }
            
            try:
                async with self.session.get(search_url, headers=headers, params=params) as response:
                    if response.status != 200:
                        print(f"Error querying Foursquare: {response.status}")
                        continue
                    
                    data = await response.json()
                    
                    for place in data.get('results', []):
                        if count >= limit:
                            break
                        
                        location_data = place.get('location', {})
                        geocode = place.get('geocodes', {}).get('main', {})
                        
                        # Build address
                        address = location_data.get('formatted_address', 'Address not available')
                        
                        # Map price level to $ format
                        price = place.get('price')
                        price_range = '$' * price if price else None
                        
                        business = Business(
                            name=place.get('name'),
                            address=address,
                            latitude=geocode.get('latitude'),
                            longitude=geocode.get('longitude'),
                            category=place.get('categories', [{}])[0].get('name'),
                            price_range=price_range,
                            average_rating=place.get('rating')
                        )
                        
                        self.aggregator.add_business(business, f"foursquare_{place.get('fsq_id')}")
                        count += 1
                
                await asyncio.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                print(f"Error crawling Foursquare: {e}")
        
        print(f"✅ Foursquare crawl complete: {count} businesses")
    
    async def crawl_yelp(self, api_key: str = None, location: str = "London, UK", limit: int = 500):
        """
        Crawl Yelp Fusion API
        500 free calls/day. Get key at: https://www.yelp.com/developers
        """
        if not api_key:
            print("⚠️  Yelp API key required")
            print("Register at: https://www.yelp.com/developers")
            return
        
        print("⭐ Starting Yelp crawl...")
        
        search_url = "https://api.yelp.com/v3/businesses/search"
        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        
        # Search different categories
        categories = [
            "restaurants", "bars", "hotels", "shopping", 
            "homeservices", "beautysvc", "health"
        ]
        
        count = 0
        for category in categories:
            if count >= limit:
                break
            
            params = {
                "location": location,
                "categories": category,
                "limit": 50
            }
            
            try:
                async with self.session.get(search_url, headers=headers, params=params) as response:
                    if response.status != 200:
                        print(f"Error querying Yelp: {response.status}")
                        continue
                    
                    data = await response.json()
                    
                    for biz in data.get('businesses', []):
                        if count >= limit:
                            break
                        
                        # Extract address
                        location_data = biz.get('location', {})
                        address = ', '.join(filter(None, [
                            location_data.get('address1'),
                            location_data.get('city'),
                            location_data.get('zip_code')
                        ]))
                        
                        business = Business(
                            name=biz.get('name'),
                            address=address,
                            latitude=biz.get('coordinates', {}).get('latitude'),
                            longitude=biz.get('coordinates', {}).get('longitude'),
                            category=category.capitalize(),
                            price_range=biz.get('price'),
                            average_rating=biz.get('rating')
                        )
                        
                        self.aggregator.add_business(business, f"yelp_{biz.get('id')}")
                        count += 1
                
                await asyncio.sleep(0.2)  # Rate limiting (500/day = ~0.17s between calls)
                
            except Exception as e:
                print(f"Error crawling Yelp: {e}")
        
        print(f"✅ Yelp crawl complete: {count} businesses")
    
    async def crawl_yell_uk(self):
        """
        Crawl Yell.com (UK business directory)
        Respects robots.txt - Web scraping approach
        """
        base_url = "https://www.yell.com"
        
        # Check robots.txt first
        can_fetch = await self.robots_checker.can_fetch(base_url)
        if not can_fetch:
            print(f"❌ Crawling blocked by robots.txt for {base_url}")
            return
        
        print("📒 Yell.com crawler - Implementation needed")
        print("   Requires BeautifulSoup/Selenium for web scraping")
        # Actual implementation would scrape categories and businesses
        # This is more complex and fragile than API-based approaches
        pass
    
    async def generate_ai_description(self, business: Business, api_key: str) -> str:
        """
        Generate AI description for business using Anthropic or OpenAI
        """
        # Simple description based on available data
        prompt = f"""Write a brief, engaging 2-sentence description for this business:

Name: {business.name}
Category: {business.category or 'business'}
Address: {business.address}
Rating: {business.average_rating or 'N/A'}
Price Range: {business.price_range or 'N/A'}

Make it sound natural and helpful for someone looking for this type of business."""

        try:
            # Try Anthropic first (if key starts with sk-ant)
            if api_key.startswith('sk-ant'):
                url = "https://api.anthropic.com/v1/messages"
                headers = {
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                }
                payload = {
                    "model": "claude-3-haiku-20240307",
                    "max_tokens": 150,
                    "messages": [{"role": "user", "content": prompt}]
                }
                
                async with self.session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['content'][0]['text'].strip()
            
            # Try OpenAI (if key starts with sk-)
            elif api_key.startswith('sk-'):
                url = "https://api.openai.com/v1/chat/completions"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                payload = {
                    "model": "gpt-3.5-turbo",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 150
                }
                
                async with self.session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['choices'][0]['message']['content'].strip()
        
        except Exception as e:
            print(f"AI generation error: {e}")
        
        # Fallback: Simple template-based description
        description = f"{business.name} is a {business.category or 'business'} located at {business.address}."
        if business.average_rating:
            description += f" It has an average rating of {business.average_rating} stars."
        return description
    
    def save_to_database(self):
        """Save all aggregated businesses to MySQL database"""
        try:
            import mysql.connector
            from mysql.connector import Error
            
            connection = mysql.connector.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            
            if connection.is_connected():
                cursor = connection.cursor()
                businesses = self.aggregator.get_all_businesses()
                
                insert_query = """
                    INSERT INTO businesses 
                    (name, address, latitude, longitude, category, opening_hours, 
                     average_rating, price_range, ai_description, source_urls, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        latitude = COALESCE(VALUES(latitude), latitude),
                        longitude = COALESCE(VALUES(longitude), longitude),
                        category = COALESCE(VALUES(category), category),
                        opening_hours = COALESCE(VALUES(opening_hours), opening_hours),
                        average_rating = VALUES(average_rating),
                        price_range = COALESCE(VALUES(price_range), price_range),
                        ai_description = COALESCE(VALUES(ai_description), ai_description),
                        source_urls = VALUES(source_urls),
                        last_updated = VALUES(last_updated)
                """
                
                for business in businesses:
                    cursor.execute(insert_query, (
                        business.name,
                        business.address,
                        business.latitude,
                        business.longitude,
                        business.category,
                        json.dumps(business.opening_hours) if business.opening_hours else None,
                        business.average_rating,
                        business.price_range,
                        business.ai_description,
                        json.dumps(business.source_urls) if business.source_urls else None,
                        business.last_updated
                    ))
                
                connection.commit()
                cursor.close()
                connection.close()
                print(f"✅ Saved {len(businesses)} businesses to MySQL database")
                
        except Error as e:
            print(f"❌ Error saving to database: {e}")
            raise
    
    def export_to_json(self, output_file: str = "uk_businesses.json"):
        """Export businesses to JSON file"""
        businesses = self.aggregator.get_all_businesses()
        data = [b.to_dict() for b in businesses]
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported {len(businesses)} businesses to {output_file}")
    
    async def run_full_crawl(self, config: dict = None):
        """Execute full crawl across all sources"""
        print("Starting UK Business Directory Crawler...")
        print("Building a Google Places API replacement!")
        print("=" * 50)
        
        # Load API keys from config or environment
        if not config:
            import os
            from dotenv import load_dotenv
            
            # Support both standalone and submodule usage
            env_paths = [
                '.env',
                '../.env',
                '../../.env',
                os.path.join(os.path.dirname(__file__), '.env'),
            ]
            
            for env_path in env_paths:
                if os.path.exists(env_path):
                    load_dotenv(env_path)
                    break
            else:
                load_dotenv()
            
            config = {
                'companies_house_key': os.getenv('COMPANIES_HOUSE_API_KEY'),
                'foursquare_key': os.getenv('FOURSQUARE_API_KEY'),
                'yelp_key': os.getenv('YELP_API_KEY'),
                'anthropic_key': os.getenv('ANTHROPIC_API_KEY'),
                'openai_key': os.getenv('OPENAI_API_KEY')
            }
        
        async with aiohttp.ClientSession() as self.session:
            # Crawl multiple sources in priority order
            crawl_tasks = []
            
            # Priority 1: Free, official sources
            if config.get('companies_house_key'):
                crawl_tasks.append(self.crawl_companies_house(config['companies_house_key'], limit=500))
            
            # Priority 2: OpenStreetMap (always available, no key needed)
            crawl_tasks.append(self.crawl_openstreetmap(limit=500))
            
            # Priority 3: Paid APIs with generous free tiers
            if config.get('foursquare_key'):
                crawl_tasks.append(self.crawl_foursquare(config['foursquare_key'], limit=500))
            
            if config.get('yelp_key'):
                crawl_tasks.append(self.crawl_yelp(config['yelp_key'], limit=500))
            
            # Run all crawlers concurrently
            await asyncio.gather(*crawl_tasks, return_exceptions=True)
        
        # Generate AI descriptions for all businesses
        print("\n🤖 Generating AI descriptions...")
        businesses = self.aggregator.get_all_businesses()
        
        ai_key = config.get('anthropic_key') or config.get('openai_key')
        if ai_key:
            for i, business in enumerate(businesses):
                if not business.ai_description:
                    business.ai_description = await self.generate_ai_description(business, ai_key)
                
                if (i + 1) % 50 == 0:
                    print(f"  Generated {i + 1}/{len(businesses)} descriptions...")
        else:
            print("  ⚠️  No AI API key found - skipping description generation")
        
        # Save results
        print("\n💾 Saving data...")
        self.save_to_database()
        self.export_to_json()
        
        print("=" * 50)
        print(f"✅ Crawl completed! Total businesses: {len(businesses)}")
        print(f"📊 Data aggregated from multiple sources")
        print(f"📁 Saved to MySQL: {self.db_config['database']} and uk_businesses.json")


async def main():
    """Main entry point"""
    crawler = BusinessCrawler()
    
    # Example: Add a sample business manually
    sample_business = Business(
        name="Sample UK Business Ltd",
        address="123 High Street, London, SW1A 1AA",
        latitude=51.5074,
        longitude=-0.1278,
        category="Restaurant",
        opening_hours={
            "Monday": "09:00-17:00",
            "Tuesday": "09:00-17:00",
            "Wednesday": "09:00-17:00",
            "Thursday": "09:00-17:00",
            "Friday": "09:00-17:00",
            "Saturday": "10:00-16:00",
            "Sunday": "Closed"
        },
        average_rating=4.5,
        price_range="$$"
    )
    
    crawler.aggregator.add_business(sample_business, "manual_entry")
    
    # Run full crawl
    await crawler.run_full_crawl()


if __name__ == "__main__":
    asyncio.run(main())

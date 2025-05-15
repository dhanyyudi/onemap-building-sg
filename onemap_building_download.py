#!/usr/bin/env python3
"""
OneMap SG Building Download Script

This script downloads building data from Singapore's OneMap API,
processes it, and saves it to a CSV file.
"""

import os
import pandas as pd
import aiohttp
import asyncio
from tqdm import tqdm
import nest_asyncio
import logging
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Apply nest_asyncio for compatibility in various environments
nest_asyncio.apply()

class OnemapDownloader:
    """Class to handle downloading building data from OneMap API"""
    
    def __init__(self, output_dir='data', output_file=None):
        """Initialize the downloader with output directory and file name"""
        self.output_dir = output_dir
        if output_file:
            self.output_file = output_file
        else:
            current_date = datetime.now().strftime('%d%m%Y')
            self.output_file = os.path.join(output_dir, f'onemap_{current_date}.csv')
        
        self.error_log_filename = os.path.join(output_dir, 'error_log.txt')
        self.df = pd.DataFrame(columns=['blk_no', 'street', 'postal_code', 'name', 'lat', 'lon'])
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

    def log_error(self, postal_code, error):
        """Log errors to file"""
        with open(self.error_log_filename, 'a') as f:
            f.write(f"Error with postal code {postal_code}: {error}\n")

    async def fetch_postal(self, session, postal_code, retries=3):
        """Fetch data for a specific postal code with retries"""
        url = f'https://www.onemap.gov.sg/api/common/elastic/search?searchVal={postal_code}&returnGeom=Y&getAddrDetails=Y&pageNum=1'
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=60) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        self.log_error(postal_code, f"HTTP {response.status}")
                        break
            except asyncio.TimeoutError:
                if attempt < retries - 1:
                    continue  # Retry
                else:
                    self.log_error(postal_code, "Timeout after retries")
            except Exception as e:
                self.log_error(postal_code, str(e))
                break
        return None

    async def process_postal(self, postal_code, session):
        """Process a single postal code and extract building information"""
        result = await self.fetch_postal(session, postal_code)
        records = []
        if result and result.get('found', 0) > 0:
            for page in range(1, result['totalNumPages'] + 1):
                url = f'https://www.onemap.gov.sg/api/common/elastic/search?searchVal={postal_code}&returnGeom=Y&getAddrDetails=Y&pageNum={page}'
                try:
                    async with session.get(url, timeout=60) as response:
                        if response.status == 200:
                            data = await response.json()
                            for item in data.get('results', []):
                                records.append({
                                    'blk_no': item.get('BLK_NO', ''),
                                    'street': item.get('ROAD_NAME', ''),
                                    'postal_code': item.get('POSTAL', ''),
                                    'name': item.get('BUILDING', ''),
                                    'lat': item.get('LATITUDE', ''),
                                    'lon': item.get('LONGITUDE', ''),
                                })
                except asyncio.TimeoutError:
                    self.log_error(postal_code, f"Page {page} timeout")
        return records

    async def download_data(self):
        """Main async function to download data for all postal codes"""
        # Generate list of all possible Singapore postal codes (6 digits)
        # Singapore postal codes range from 010000 to 829999
        postal_codes = [f"{i:06d}" for i in range(10000, 830000)]
        
        logger.info(f"Starting download of OneMap data for {len(postal_codes)} postal codes")
        
        sem = asyncio.Semaphore(20)  # Limit concurrency to 20 tasks
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i, postal_code in enumerate(postal_codes):
                async with sem:
                    tasks.append(self.process_postal(postal_code, session))

                # Save progress periodically and clear tasks to manage memory
                if i % 1000 == 0 and i > 0:
                    logger.info(f"Processing progress: {i}/{len(postal_codes)} postal codes, current records: {len(self.df)}")
                    completed = await asyncio.gather(*tasks)
                    for batch in completed:
                        if batch:
                            self.df = pd.concat([self.df, pd.DataFrame(batch)], ignore_index=True)
                    tasks.clear()

            # Final save for remaining tasks
            if tasks:
                completed = await asyncio.gather(*tasks)
                for batch in completed:
                    if batch:
                        self.df = pd.concat([self.df, pd.DataFrame(batch)], ignore_index=True)

        logger.info(f"Download complete. Total records: {len(self.df)}")
        return self.df

    def save_data(self):
        """Save downloaded data to CSV file"""
        self.df.to_csv(self.output_file, index=False)
        logger.info(f"Data saved to {self.output_file}")
        logger.info(f"Total records saved: {len(self.df)}")

    async def run(self):
        """Run the entire download process"""
        await self.download_data()
        self.save_data()
        return self.df

def main():
    """Main function to handle command line arguments and execute download"""
    parser = argparse.ArgumentParser(description='Download building data from OneMap API')
    parser.add_argument('--output_dir', type=str, default='data', 
                        help='Directory to save output files')
    parser.add_argument('--output_file', type=str, default=None,
                        help='Output CSV filename (default: onemap_DDMMYYYY.csv)')
    
    args = parser.parse_args()
    
    # Create downloader instance
    downloader = OnemapDownloader(args.output_dir, args.output_file)
    
    # Run the download process
    asyncio.run(downloader.run())

if __name__ == "__main__":
    main()

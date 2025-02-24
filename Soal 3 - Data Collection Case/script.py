import asyncio
import re
import httpx
import polars as pl
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.fortiguard.com/encyclopedia"
MAX_RETRIES = 3
TIMEOUT = 30.0
RISK_LEVELS = [1, 2, 3, 4, 5]
MAX_PAGES_PER_LEVEL = 10
DATASETS_DIR = Path("datasets")


@dataclass(frozen=True)
class FortiEntry:
    title: str
    link: str


class FortiGuardScraper:
    def __init__(self):
        self.skipped_pages = {level: [] for level in RISK_LEVELS}

        # Ensure datasets directory exists
        DATASETS_DIR.mkdir(exist_ok=True)

        # Initialize client with custom headers and timeout
        self.client_kwargs = {
            "timeout": TIMEOUT,
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        }
        self.client = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(**self.client_kwargs)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    @retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def fetch_page(self, client: httpx.AsyncClient, level: int, page: int) -> Optional[str]:
        """Fetch a single page with retries."""
        url = f"{BASE_URL}?type=ips&risk={level}&page={page}"
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            self.skipped_pages[level].append((page, str(e)))
            raise

    def parse_page(self, html: str) -> List[FortiEntry]:
        """Parse the HTML content and extract required information."""
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        entries = []

        try:
            row_divs = self._find_row_divs(soup)
            entries = [self._extract_entry(row) for row in row_divs if self._is_valid_row(row)]
        except Exception as e:
            logger.error(f"Error parsing page: {str(e)}")

        return entries

    def _find_row_divs(self, soup: BeautifulSoup):
        return soup.find_all('div', class_='row', onclick=lambda x: x and 'location.href' in x)

    def _is_valid_row(self, row) -> bool:
        return bool(row.get('onclick')) and bool(row.find('b'))

    def _extract_entry(self, row) -> FortiEntry:
        link = row.get('onclick')
        match = re.search(r"/encyclopedia/([^']+)'", link)
        link_path = match.group(1) if match else ""
        full_link = f'{BASE_URL}/{link_path}'

        title = row.find('b').text

        return FortiEntry(title=title, link=full_link)

    async def scrape_level(self, client: httpx.AsyncClient, level: int) -> List[FortiEntry]:
        """Scrape all pages for a specific risk level."""
        all_entries = []
        tasks = []

        for page in range(1, MAX_PAGES_PER_LEVEL + 1):
            task = asyncio.create_task(self.fetch_page(client, level, page))
            tasks.append(task)

        for completed_task in asyncio.as_completed(tasks):
            try:
                page_content = await completed_task
                if page_content:
                    entries = self.parse_page(page_content)
                    all_entries.extend(entries)
            except Exception as e:
                logger.error(f"Error processing page for level {level}: {str(e)}")

        return all_entries

    def save_to_csv(self, entries: List[FortiEntry], level: int):
        """Save scraped data to a CSV file using polars."""
        if not entries:
            logger.warning(f"No data to save for level: {level}")
            return

        try:
            df = pl.DataFrame([asdict(entry) for entry in entries])
            output_path = DATASETS_DIR / f"forti_lists_{level}.csv"
            df.write_csv(output_path)
            logger.info(f"Saved {len(entries)} entries to {output_path}")
        except Exception as e:
            logger.error(f"Error saving CSV for level {level}: {str(e)}")

    def save_skipped_pages(self):
        """Save information about skipped pages to JSON."""
        try:
            output_path = DATASETS_DIR / "skipped.json"
            with open(output_path, 'w') as f:
                json.dump(self.skipped_pages, f, indent=2)
            logger.info(f"Saved skipped pages information to {output_path}")
        except Exception as e:
            logger.error(f"Error saving skipped pages: {str(e)}")

    async def run(self):
        """Main execution method."""
        async with httpx.AsyncClient(**self.client_kwargs) as client:
            for level in RISK_LEVELS:
                logger.info(f"Starting scraping for risk level: {level}")
                entries = await self.scrape_level(client, level)
                self.save_to_csv(entries, level)

        self.save_skipped_pages()
        logger.info("Scraping completed")


async def main():
    async with FortiGuardScraper() as scraper:
        await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
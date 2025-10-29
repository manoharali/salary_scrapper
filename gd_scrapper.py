#!/usr/bin/env python3
"""
Glassdoor Job Scraper (gd_scrapper.py)
- Form-based search (like new_scraper.py)
- Batch processing with concurrent scraping (like scraper_fast.py)
- Progress tracking and logging
- Speed optimized for large-scale scraping
"""

import argparse
import asyncio
import re
import os
import time
import logging
from datetime import datetime

import unicodecsv as csv
from lxml import html
from playwright.async_api import Playwright, async_playwright


# Setup logging
def setup_logging():
    """Setup logging for progress tracking"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"gd_scrapper_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


async def run(playwright: Playwright, keyword: str, place: str, logger):
    """Collecting details of all jobs in the provided keyword and place"""
    
    logger.info(f"[START] Starting scraping: {keyword} in {place}")
    start_time = time.time()
    
    # Launch browser in headless mode for speed
    browser = await playwright.chromium.launch(
        headless=False,  # No browser window
        args=[
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-images',
            '--disable-plugins',
            '--disable-extensions'
        ]
    )
    
    context = await browser.new_context(
        viewport={'width': 1280, 'height': 720},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    )
    
    # Override geographic location to prevent IP-based redirects
    if 'new-york' in place.lower():
        await context.set_geolocation({"latitude": 40.7128, "longitude": -74.0060})  # NYC
    elif 'hyderabad' in place.lower():
        await context.set_geolocation({"latitude": 17.3850, "longitude": 78.4867})  # Hyderabad
    elif 'mumbai' in place.lower():
        await context.set_geolocation({"latitude": 19.0760, "longitude": 72.8777})  # Mumbai
    elif 'bangalore' in place.lower():
        await context.set_geolocation({"latitude": 12.9716, "longitude": 77.5946})  # Bangalore
    elif 'boston' in place.lower():
        await context.set_geolocation({"latitude": 42.3601, "longitude": -71.0589})  # Boston
    
    # Grant permissions after geolocation
    await context.grant_permissions(["geolocation"])
    await context.set_extra_http_headers({"Accept-Language": "en-US"})
    
    page = await context.new_page()
    
    try:
        # Form-based search like new_scraper.py
        logger.info(f"[LOAD] Navigating to Glassdoor job search page...")
        await page.goto("https://www.glassdoor.com/Job/index.htm", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(timeout=3000)  # Wait for page to fully load
        
        # Find and fill job title field
        logger.info(f"[INPUT] Entering job title: {keyword}")
        job_title_input = page.locator('#searchBar-jobTitle')
        await job_title_input.click()
        await job_title_input.fill(keyword)
        await page.wait_for_timeout(timeout=1000)  # Wait for autocomplete
        
        # Find and fill location field - convert place to readable format
        # Handle formats like "new-york-ny" -> "New York"
        location_text = place.replace('-', ' ').title()
        if 'ny' in place.lower():
            location_text = location_text.replace('Ny', 'NY')
        
        logger.info(f"[INPUT] Entering location: {location_text}")
        location_input = page.locator('#searchBar-location')
        await location_input.click()
        await location_input.fill(location_text)
        await page.wait_for_timeout(timeout=2000)  # Wait for location suggestions
        
        # Wait for location dropdown and select first suggestion
        logger.info(f"[DROPDOWN] Waiting for location suggestions...")
        try:
            await page.wait_for_selector('#searchBar-location-search-suggestions li', timeout=5000)
            first_suggestion = page.locator('#searchBar-location-search-suggestions li').first
            await first_suggestion.click()
            logger.info(f"[SELECTED] Selected first location suggestion")
            await page.wait_for_timeout(timeout=1000)
        except Exception as e:
            logger.warning(f"[WARNING] Could not find location suggestions: {str(e)}")
        
        # Submit the form
        logger.info(f"[SUBMIT] Submitting search form...")
        try:
            submit_button = page.locator('button[type="submit"]')
            if await submit_button.is_visible():
                await submit_button.click()
            else:
                await location_input.press('Enter')
        except:
            await location_input.press('Enter')
        
        # Wait for results page to load
        logger.info(f"[WAIT] Waiting for search results...")
        await page.wait_for_timeout(timeout=3000)
        
        # Wait for job listings to appear
        try:
            await page.wait_for_selector('[class*="JobCard"]', timeout=10000)
            logger.info(f"[LOADED] Search results page loaded")
        except:
            logger.warning(f"[WARNING] Job cards not found, continuing anyway...")
        
        # Scroll to load more jobs
        logger.info(f"[SCROLL] Scrolling to load more jobs...")
        for _ in range(5):  # Scroll multiple times to load more
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(timeout=2000)
        
        # Get page content and extract job links
        response = await page.content()
        tree = html.fromstring(response)
        
        # Collecting urls to each job description page - try multiple selectors
        links = tree.xpath('//a[contains(@href, "/job-listing/")]/@href')
        if not links:
            links = tree.xpath('//a[contains(@class, "JobCard")]/@href')
        if not links:
            links = tree.xpath('//a[contains(@href, "glassdoor.com/job-listing")]/@href')
        if not links:
            links = tree.xpath('//a[@class="JobCard_jobTitle__rbjTE"]/@href')
        if not links:
            links = tree.xpath('//a[contains(@class, "jobTitle")]/@href')
        
        logger.info(f"[FOUND] Found {len(links)} job links")
        
        # Make links absolute and smart limit based on results
        base_url = "https://www.glassdoor.com"
        total_found = len(links)
        
        # Smart scraping: if 20+ results, scrape more (up to 60), otherwise scrape all
        if total_found >= 20:
            scrape_limit = min(60, total_found)  # Max 60 for safety
        else:
            scrape_limit = total_found  # Scrape all if less than 20
        
        links = [link if link.startswith("http") else base_url + link for link in links[:scrape_limit]]
        
        logger.info(f"[PROCESS] Processing {len(links)} jobs...")
        
        job_listings = []
        
        # Process jobs in smaller batches for stability
        batch_size = 5  # Reduced batch size for better stability
        total_jobs = len(links)
        successful_jobs = 0
        
        print(f"Smart scraping: {total_found} found -> scraping {len(links)} jobs")
        
        for batch_start in range(0, len(links), batch_size):
            batch_links = links[batch_start:batch_start + batch_size]
            batch_jobs = await process_batch(context, batch_links, batch_start + 1, logger)
            job_listings.extend(batch_jobs)
            successful_jobs += len(batch_jobs)
            
            # Clean progress update
            processed = min(batch_start + batch_size, len(links))
            print(f"Progress: {processed}/{total_jobs} jobs processed | {successful_jobs} successful")
            
            # Increased delay for stability
            await asyncio.sleep(1.0)
        
        await browser.close()
        
        # Location filter disabled - return all jobs
        elapsed_time = time.time() - start_time
        if len(job_listings) > 0:
            logger.info(f"[COMPLETE] Completed {keyword}-{place}: {len(job_listings)} jobs in {elapsed_time:.2f}s")
        else:
            logger.warning(f"[COMPLETE] No jobs found for {keyword}-{place}.")
        
        return job_listings
        
    except Exception as e:
        logger.error(f"Error in {keyword}-{place}: {str(e)}")
        await browser.close()
        return []


def filter_jobs_by_location(job_listings, place):
    """Filter jobs to only include those matching the target location"""
    if not job_listings:
        return job_listings
    
    # Extract expected location keywords from place parameter
    place_lower = place.lower()
    expected_keywords = []
    
    # Country-level mappings
    if 'canada' in place_lower:
        expected_keywords = ['canada', 'toronto', 'vancouver', 'montreal', 'calgary', 'ottawa', 
                           'edmonton', 'winnipeg', 'quebec', 'on', 'bc', 'qc', 'ab', 'mb', 'sk']
    elif 'united states' in place_lower or 'usa' in place_lower or 'us' in place_lower:
        # Don't filter - too broad
        return job_listings
    # Common city mappings
    elif 'new-york' in place_lower or 'ny' in place_lower:
        expected_keywords = ['new york', 'nyc', 'new-york', 'ny']
    elif 'boston' in place_lower:
        expected_keywords = ['boston', 'ma']
    elif 'hyderabad' in place_lower:
        expected_keywords = ['hyderabad']
    elif 'mumbai' in place_lower:
        expected_keywords = ['mumbai']
    elif 'bangalore' in place_lower:
        expected_keywords = ['bangalore', 'bengaluru']
    else:
        # Generic: extract city name from place parameter
        city_name = place_lower.split('-')[0] if '-' in place_lower else place_lower
        expected_keywords = [city_name]
    
    filtered_jobs = []
    for job in job_listings:
        location_lower = job.get('Location', '').lower()
        city_lower = job.get('City', '').lower()
        state_lower = job.get('State', '').lower()
        region_lower = job.get('Region', '').lower()
        
        # Check if any location field contains expected keywords
        matches = False
        for keyword in expected_keywords:
            if keyword in location_lower or keyword in city_lower or keyword in region_lower or keyword in state_lower:
                matches = True
                break
        
        if matches:
            filtered_jobs.append(job)
    
    return filtered_jobs


async def process_batch(context, links, start_idx, logger):
    """Process a batch of job links concurrently"""
    tasks = []
    
    for idx, link in enumerate(links, start_idx):
        task = process_single_job(context, link, idx, logger)
        tasks.append(task)
    
    # Process batch concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions and None values, return only valid jobs
    valid_jobs = []
    for result in results:
        if isinstance(result, dict) and result is not None:
            valid_jobs.append(result)
        elif isinstance(result, Exception):
            logger.warning(f"⚠️ Job processing failed: {str(result)}")
        # Skip None values (failed jobs)
    
    return valid_jobs


async def process_single_job(context, link, idx, logger):
    """Process a single job page"""
    try:
        page = await context.new_page()
        # Retry logic for failed pages
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await page.goto(link, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(timeout=500)  # Increased delay for stability
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                await page.wait_for_timeout(timeout=1000)  # Wait before retry
        
        response = await page.content()
        tree = html.fromstring(response)
        
        # Extract job data (same logic as scraper_fast.py)
        job_data = extract_job_data(tree, link)
        
        await page.close()
        
        return job_data
        
    except Exception as e:
        logger.warning(f"⚠️ Failed job {idx}: {str(e)[:100]}")
        # Return empty job data instead of N/A values
        return None


def extract_job_data(tree, link):
    """Extract job data from HTML tree"""
    # Job title
    role_elements = tree.xpath('//div[@class="JobDetails_jobDetailsHeader__qKuvs"]/h1/text()')
    if not role_elements:
        role_elements = tree.xpath('//h1[contains(@class, "jobTitle")]/text()')
    if not role_elements:
        role_elements = tree.xpath('//h1/text()')
    role = role_elements[0].strip() if role_elements else "N/A"
    
    # Company name - try multiple selectors
    company_elements = tree.xpath('//div[@class="JobDetails_jobDetailsHeader__qKuvs"]/a/div/span/text()')
    if not company_elements:
        company_elements = tree.xpath('//span[contains(@class, "employerName")]/text()')
    if not company_elements:
        company_elements = tree.xpath('//a[contains(@class, "employerName")]/text()')
    if not company_elements:
        # Try to get from href or data attributes
        company_elements = tree.xpath('//a[contains(@class, "employerName")]//text()')
    if not company_elements:
        # Extract from URL as fallback
        try:
            if "/job-listing/" in link and "-JV_" in link:
                # Pattern: .../job-listing/job-title-company-name-JV_...
                url_part = link.split("/job-listing/")[1].split("-JV_")[0]
                words = url_part.split("-")
                # Company name is usually at the end, try last 2-3 words
                if len(words) > 2:
                    # Try last 2 words as company name
                    company_parts = words[-2:]
                    company_name = " ".join(company_parts).title()
                    company_elements = [company_name]
            elif "/job-listing/" in link:
                # Simpler pattern without JV_
                url_part = link.split("/job-listing/")[1].split("?")[0]
                words = url_part.split("-")
                if len(words) > 2:
                    company_parts = words[-2:]
                    company_name = " ".join(company_parts).title()
                    company_elements = [company_name]
        except:
            pass
    company_name = company_elements[0].strip() if company_elements else "N/A"
    
    # Location
    location_elements = tree.xpath('//div[@class="JobDetails_jobDetailsHeader__qKuvs"]/div/text()')
    if not location_elements:
        location_elements = tree.xpath('//div[contains(@class, "location")]/text()')
    location = location_elements[0].strip() if location_elements else "N/A"
    
    # Parse city and state
    try:
        if "," in location:
            parts = [p.strip() for p in location.split(",")]
            city = parts[0] if len(parts) > 0 else "N/A"
            state = parts[1] if len(parts) > 1 else "N/A"
        else:
            city = location
            state = "N/A"
    except:
        city = "N/A"
        state = "N/A"
    
    # Year
    year = "N/A"
    try:
        page_text = tree.xpath('//text()')
        page_text_str = " ".join(page_text)
        year_match = re.search(r'(20\d{2})', page_text_str)
        if year_match:
            year = year_match.group(1)
    except:
        pass
    
    # Salary
    salary_elements = tree.xpath('//div[@class="SalaryEstimate_averageEstimate__xF_7h"]/text()')
    if not salary_elements:
        salary_elements = tree.xpath('//span[contains(@class, "salary")]/text()')
    if not salary_elements:
        salary_elements = tree.xpath('//div[contains(@class, "SalaryEstimate")]//text()')
    
    salary = "N/A"
    if salary_elements:
        salary_text = " ".join(salary_elements).strip()
        min_match = re.search(r'minimum salary is \$(\d+)K', salary_text)
        max_match = re.search(r'max salary is \$(\d+)K', salary_text)
        
        if min_match and max_match:
            min_sal = min_match.group(1) + "K"
            max_sal = max_match.group(1) + "K"
            salary = f"${min_sal} - ${max_sal}"
        elif min_match:
            min_sal = min_match.group(1) + "K"
            salary = f"${min_sal}"
        else:
            # Clean salary text by removing location references
            salary = salary_text[:100]  # Take first 100 characters
            # Remove common location patterns
            for location_pattern in ["Boston, MA", "New York, NY", "Hyderabad, India", "San Francisco, CA"]:
                if location_pattern in salary:
                    salary = salary.split(location_pattern)[0].strip()
                    break
    
    # Currency - Worldwide detection
    currency = "USD"  # Default
    if "$" in salary:
        currency = "USD"
    elif "€" in salary:
        currency = "EUR"
    elif "£" in salary:
        currency = "GBP"
    elif "¥" in salary:
        currency = "JPY"
    elif "₹" in salary or "INR" in salary.upper():
        currency = "INR"
    elif "CAD" in salary.upper():
        currency = "CAD"
    elif "AUD" in salary.upper():
        currency = "AUD"
    elif "CHF" in salary.upper():
        currency = "CHF"
    elif "SEK" in salary.upper():
        currency = "SEK"
    elif "NOK" in salary.upper():
        currency = "NOK"
    elif "DKK" in salary.upper():
        currency = "DKK"
    elif "PLN" in salary.upper():
        currency = "PLN"
    elif "CZK" in salary.upper():
        currency = "CZK"
    elif "HUF" in salary.upper():
        currency = "HUF"
    elif "RUB" in salary.upper():
        currency = "RUB"
    elif "BRL" in salary.upper():
        currency = "BRL"
    elif "MXN" in salary.upper():
        currency = "MXN"
    elif "ZAR" in salary.upper():
        currency = "ZAR"
    elif "KRW" in salary.upper():
        currency = "KRW"
    elif "SGD" in salary.upper():
        currency = "SGD"
    elif "HKD" in salary.upper():
        currency = "HKD"
    elif "NZD" in salary.upper():
        currency = "NZD"
    
    # Region
    region = location if location != "N/A" else f"{city}, {state}" if city != "N/A" and state != "N/A" else "N/A"
    
    # Years of Experience
    years_of_experience = "N/A"
    try:
        requirements_text = tree.xpath('//div[contains(@class, "JobDetails_jobDescription")]//text()')
        if not requirements_text:
            requirements_text = tree.xpath('//div[contains(@class, "jobDescription")]//text()')
        if not requirements_text:
            requirements_text = tree.xpath('//text()')
        
        page_text_str = " ".join(requirements_text)
        
        exp_patterns = [
            r'(\d+)\+?\s*years?\s*of?\s*experience',
            r'minimum\s+of?\s*(\d+)\s*years?',
            r'at\s+least\s+(\d+)\s*years?',
            r'(\d+)[-–](\d+)\s*years?\s*(of\s*)?experience',
            r'(\d+)\s*years?\s*(of\s*)?experience',
        ]
        
        for pattern in exp_patterns:
            matches = re.finditer(pattern, page_text_str, re.IGNORECASE)
            for match in matches:
                groups = match.groups()
                if len(groups) == 2 and all(g and g.isdigit() for g in groups):
                    years_of_experience = f"{groups[0]}-{groups[1]} years"
                    break
                elif len(groups) >= 1 and groups[0] and groups[0].isdigit():
                    years_of_experience = f"{groups[0]}+ years"
                    break
            if years_of_experience != "N/A":
                break
    except:
        pass
    
    return {
        "Name": role,
        "Company": company_name,
        "State": state,
        "City": city,
        "Salary": salary,
        "Location": location,
        "Currency": currency,
        "Region": region,
        "Years of Experience": years_of_experience,
        "Year": year,
        "Url": link,
    }


async def main(keyword: str, place: str):
    """Main async function"""
    logger = setup_logging()
    
    print("="*60)
    print(f"GLASSDOOR JOB SCRAPER (gd_scrapper.py)")
    print(f"Target: {keyword} in {place}")
    print(f"Started at: {datetime.now().strftime('%H:%M:%S')}")
    print("="*60)
    
    async with async_playwright() as playwright:
        job_listings = await run(playwright, keyword, place, logger)
        return job_listings


def parse(keyword: str, place: str):
    """Parse and return job listings"""
    job_listings = asyncio.run(main(keyword, place))
    return job_listings


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Glassdoor Job Scraper - Form Search + Batch Processing")
    parser.add_argument("keyword", help="Job title (e.g., data-scientist)", type=str)
    parser.add_argument("place", help="Location (e.g., new-york-ny)", type=str)
    
    args = parser.parse_args()
    keyword = args.keyword
    place = args.place
    
    # Scrape data
    scraped_data = parse(keyword, place)
    
    # Save to CSV
    output_file = f"{keyword}-{place}-results.csv"
    
    print("\n" + "="*60)
    print("Writing to CSV file...")
    print("="*60)
    
    with open(output_file, "wb") as csvfile:
        fieldnames = ["Name", "Company", "State", "City", "Salary", "Location", "Currency", "Region", "Years of Experience", "Year", "Url"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        if scraped_data:
            for data in scraped_data:
                writer.writerow(data)
            print(f"Successfully saved {len(scraped_data)} jobs to: {output_file}")
        else:
            print("No data to save.")
    
    print("\n" + "="*60)


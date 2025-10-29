import csv
import os
from gd_scrapper import parse

def read_csv(filename):
    """Read CSV file and return list of dictionaries"""
    with open(filename, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def main():
    # Read the CSV files
    try:
        locations = read_csv('country.csv')
        jobs = read_csv('jobs.csv')
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    # Process each job and location combination
    for job in jobs:
        job_title = job.get('job_title', '').strip()
        if not job_title:
            continue

        for location in locations:
            city = location.get('city', '').strip()
            country = location.get('country', '').strip()
            
            if not city or not country:
                continue

            # Format location as "city-country" (adjust format as needed)
            location_str = f"{city.lower().replace(' ', '-')}-{country.lower()}"
            
            print("\n" + "="*60)
            print(f"Scraping: {job_title} in {location_str}")
            print("="*60)
            
            # Run the scraper
            try:
                parse(job_title, location_str)
            except Exception as e:
                print(f"Error scraping {job_title} in {location_str}: {str(e)}")

if __name__ == "__main__":
    main()
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

            # Format location as "city-country" (replace spaces with hyphens)
            location_str = f"{city.lower().replace(' ', '-')}-{country.lower().replace(' ', '-')}"
            
            # Set the parameters as requested
            batch_size = 07
            save_interval = 50
            max_records = 500
            max_show_more_clicks = 17
            
            print(f"\n{'='*60}")
            print(f"Scraping: {job_title} in {location_str}")
            print(f"Batch size: {batch_size}, Save interval: {save_interval}")
            print(f"Max records: {max_records}, Show more clicks: {max_show_more_clicks}")
            print("="*60)
            
            # Run the scraper with the specified parameters
            try:
                parse(
                    job_title, 
                    location_str,
                    batch_size=batch_size,
                    save_interval=save_interval,
                    max_records=max_records,
                    max_show_more_clicks=max_show_more_clicks
                )
            except Exception as e:
                print(f"Error processing {job_title} in {location_str}: {str(e)}")
                # Print traceback for better error diagnosis
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    main()
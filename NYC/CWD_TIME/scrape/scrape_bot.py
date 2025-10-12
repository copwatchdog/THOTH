import requests
from bs4 import BeautifulSoup
import csv
import datetime

def scrape_nypd_trials():
    # URL of the NYPD Trials page (replace with the actual URL)
    url = "https://www.nyc.gov/site/nypd/news/trials.page"
    
    # Send a request to the NYPD Trials page
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract trial data (this will depend on the actual structure of the page)
    trials = []
    for trial in soup.select('.trial-entry'):  # Adjust the selector based on actual HTML
        date = trial.select_one('.trial-date').get_text(strip=True)
        description = trial.select_one('.trial-description').get_text(strip=True)
        trials.append({'date': date, 'description': description})

    return trials

def save_to_csv(trials, csv_path):
    # Save the scraped trials to a CSV file
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=['date', 'description'])
        writer.writeheader()
        for trial in trials:
            writer.writerow(trial)

def main():
    today = datetime.date.today()
    trials = scrape_nypd_trials()
    
    # Define the CSV path
    csv_path = "/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv"
    
    # Save the trials to CSV
    save_to_csv(trials, csv_path)
    print(f"Scraped {len(trials)} trials and saved to {csv_path}")

if __name__ == "__main__":
    main()
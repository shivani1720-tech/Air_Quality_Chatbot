#Data Scraping from air quality ontario website from 2012 to 2025
import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import nest_asyncio
import random
import time

# Apply fix for running asyncio in Jupyter Notebook
nest_asyncio.apply()

# URL for air quality data
BASE_URL = "https://www.airqualityontario.com/history/summary.php"

# Define years, months, days, and hours to scrape
YEARS = range(2012, 2013)
MONTHS = range(1, 13)
DAYS = range(1, 32)
HOURS = range(0, 24)

# List to store data
data_list = []

# Limit concurrent requests
SEMAPHORE = asyncio.Semaphore(5)  # Limit concurrent requests

async def fetch_data(session, year, month, day, hour, retries=3):
    """
    Fetch air quality data asynchronously for a given date and time.
    Retries if request fails.
    """
    params = {
        "start_year": str(year),
        "start_month": str(month),
        "start_day": str(day),
        "my_hour": str(hour),
    }

    async with SEMAPHORE:  # Limit concurrency
        for attempt in range(retries):
            try:
                async with session.get(BASE_URL, params=params) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")

                        # Find table
                        table = soup.find("table", {"class": "resourceTable"})
                        if table:
                            rows = table.find_all("tr")
                            for row in rows[1:]:  # Skip header row
                                cols = row.find_all("td")
                                row_data = [year, month, day, hour] + [col.text.strip() for col in cols]
                                data_list.append(row_data)
                        return  # Exit function on success
            except Exception as e:
                print(f"Retry {attempt+1}: Failed to fetch {year}-{month}-{day} {hour}:00 ({e})")
                await asyncio.sleep(random.uniform(2, 5))  # Random delay before retry

        print(f"❌ Final failure: {year}-{month}-{day} {hour}:00")  # Log final failure

async def main():
    """
    Main function to run all fetch requests asynchronously.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for year in YEARS:
            for month in MONTHS:
                for day in DAYS:
                    for hour in HOURS:
                        tasks.append(fetch_data(session, year, month, day, hour))

        # Run all requests in parallel with limited concurrency
        await asyncio.gather(*tasks)

# Run the async scraper properly in Jupyter
loop = asyncio.get_event_loop()
loop.run_until_complete(main())

# Convert to DataFrame and dynamically infer column names
max_cols = max(len(row) for row in data_list)  # Find max column count
columns = ["Year", "Month", "Day", "Hour"] + [f"Column{i}" for i in range(1, max_cols - 3)]

df = pd.DataFrame(data_list, columns=columns)
#df.to_csv("air_quality_data.csv", index=False)

#print("✅ Scraping complete! Data saved as 'air_quality_data.csv'.")

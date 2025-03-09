import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
from dateutil import rrule
from datetime import datetime
import re

# List of selected station names to scrape
selected_stations = [
    "BARRIE LANDFILL", "BELLEVILLE", "BRANTFORD MOE", "BURLINGTON PIERS (AUT)", "CHATHAM KENT",
    "CORNWALL", "GUELPH TURFGRASS INSTITUTE", "HAMILTON A", "KINGSTON A", "KITCHENER/WATERLOO",
    "LONDON CS", "PA HERSHEY CENTRE", "MORRISBURG", "KING CITY NORTH", "NORTH BAY A", "OAKVILLE TWN",
    "OSHAWA", "OTTAWA CDA", "OTTAWA INTL A", "PARRY SOUND HARBOUR", "PETAWAWA A",
    "PETERBOROUGH A", "SARNIA CHRIS HADFIELD A", "SAULT STE MARIE A", "ST CATHARINES BROCK U",
    "SUDBURY CLIMATE", "THUNDER BAY A", "TORONTO CITY", "TORONTO CITY CENTRE", "TORONTO NORTH YORK",
    "TORONTO LESTER B. PEARSON INT'L A", "WINDSOR A", "WINDSOR RIVERSIDE"
]

# Function to fetch station IDs for Ontario (ON)
def get_station_ids(province="ON", start_year="2019", end_year="2019", max_pages=5):
    station_data = []
    for i in range(max_pages):
        startRow = 1 + i * 100
        print(f"Downloading Page: {i}")
        
        base_url = "https://climate.weather.gc.ca/historical_data/search_historic_data_stations_e.html?"
        queryProvince = f"searchType=stnProv&timeframe=1&lstProvince={province}&optLimit=yearRange&"
        queryYear = f"StartYear={start_year}&EndYear={end_year}&Year={end_year}&Month=12&Day=31&selRowPerPage=100&"
        queryStartRow = f"startRow={startRow}"
        
        response = requests.get(base_url + queryProvince + queryYear + queryStartRow)
        soup = BeautifulSoup(response.text, 'html.parser')
        forms = soup.find_all("form", {"id": re.compile('stnRequest*')})

        for form in forms:
            try:
                station_id = form.find("input", {"name": "StationID"})['value']
                station_name = form.find("div", class_="col-md-10 col-sm-8 col-xs-8").text.strip()
                years = form.find("select", {"name": "Year"}).find_all()
                min_year = years[0].text.strip()
                max_year = years[-1].text.strip()

                # Append only if the station is in the selected list
                if station_name in selected_stations:
                    station_data.append([station_id, station_name, province, min_year, max_year])

            except Exception as e:
                print("Error processing form:", e)
                pass
    
    return pd.DataFrame(station_data, columns=['StationID', 'Name', 'Province', 'Year Start', 'Year End'])

# Function to fetch weather data for a station and check if TIME LST is present
def check_hourly_data_availability(stationID, year, month):
    base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
    query_url = f"format=csv&stationID={stationID}&Year={year}&Month={month}&timeframe=1"
    api_endpoint = base_url + query_url
    
    try:
        df = pd.read_csv(api_endpoint, skiprows=0)
        # Check if TIME LST column is present
        if 'Time (LST)' in df.columns or any("Time" in col for col in df.columns):
            return True
        return False
    except Exception as e:
        print(f"Error checking hourly data for station {stationID}: {e}")
        return False

# Function to fetch weather data for a station
def get_hourly_data(stationID, year, month):
    base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
    query_url = f"format=csv&stationID={stationID}&Year={year}&Month={month}&timeframe=1"
    api_endpoint = base_url + query_url
    
    try:
        df = pd.read_csv(api_endpoint, skiprows=0)
        
        # Ensure TIME LST column exists
        if 'Time (LST)' not in df.columns and not any("Time" in col for col in df.columns):
            return None

        return df
    except Exception as e:
        print(f"Error fetching data for station {stationID}: {e}")
        return None

# Fetch station IDs for Ontario
df_stations = get_station_ids()

# **Check if any stations were found**
if df_stations.empty:
    print("No stations found. Exiting.")
    exit()

print(f"Total selected stations found: {df_stations.shape[0]}")

# **Filter stations where hourly data is available**
available_stations = []
start_year = 2019  # Checking hourly data for this year
start_month = 1    # Checking January data

for index, row in df_stations.iterrows():
    stationID = row["StationID"]
    station_name = row["Name"]
    
    if check_hourly_data_availability(stationID, start_year, start_month):
        available_stations.append(row)

# Convert list to DataFrame
df_available_stations = pd.DataFrame(available_stations)

# **Fetch weather data for selected stations**
start_date = datetime.strptime('2019-01', '%Y-%m')
end_date = datetime.strptime('2019-12', '%Y-%m')

weather_data_list = []

for index, row in df_available_stations.iterrows():
    stationID = row["StationID"]
    station_name = row["Name"]
    print(f"Fetching data for station: {station_name} (ID: {stationID})")

    frames = []
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
        df = get_hourly_data(stationID, dt.year, dt.month)
        if df is not None:
            df["Station Name"] = station_name  # Add station name to the dataframe
            frames.append(df)

    if frames:
        station_weather_data = pd.concat(frames, ignore_index=True)
        weather_data_list.append(station_weather_data)

# **Combine all station data into a single DataFrame**
if weather_data_list:
    weather_data = pd.concat(weather_data_list, ignore_index=True)

    # Handling 'Date/Time' issue
    date_col = [col for col in weather_data.columns if 'Date' in col or 'Time' in col][0]
    weather_data[date_col] = pd.to_datetime(weather_data[date_col])

    # **Save final data**
    weather_data.to_csv("meteorological_weather_data_hourly_stations_2019.csv", index=False)
    print("Weather data saved as weather_data_hourly_stations.csv")
else:
    print("No data collected for any station.")

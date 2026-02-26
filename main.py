# ==============================================================================
# Automated GitHub Actions Script: Sri Lanka Irrigation Rainfall Data Scraper 
# ==============================================================================
import requests
import pandas as pd
import os
from datetime import timedelta

# The exact layer you found, but ending in /query instead of /queryBins
API_URL = "https://services3.arcgis.com/J7ZFXmR8rSmQ3FGf/arcgis/rest/services/gauges_2_view/FeatureServer/0/query"

def scrape_rainfall_data(url):
    print("🚀 Connecting to SL Irrigation Database...")
    
    # Parameters for the ArcGIS REST API
    params = {
        'where': '1=1',                 # '1=1' means get ALL records (no filters)
        'outFields': '*',               # Get all columns
        'f': 'json',                    # Return data as JSON
        'orderByFields': 'OBJECTID ASC',# Order by ID to ensure perfect pagination
        'resultOffset': 0,              # Start at record 0
        'resultRecordCount': 2000       # Grab 2000 records at a time
    }

    all_data = []

    while True:
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"❌ HTTP Error: {response.status_code}")
            break
            
        data = response.json()
        
        if 'error' in data:
            print(f"❌ API Error: {data['error']}")
            break
            
        features = data.get('features', [])
        if not features:
            break
            
        all_data.extend([f['attributes'] for f in features])
        
        if data.get('exceededTransferLimit'):
            params['resultOffset'] += params['resultRecordCount']
            print(f"⬇️ Downloaded {len(all_data)} records... fetching more...")
        else:
            print(f"✅ Download complete! Total records: {len(all_data)}")
            break

    if not all_data:
        print("No data found.")
        return None

    # Convert to Pandas DataFrame
    df = pd.DataFrame(all_data)
    
    # --- Clean up the Dataset ---
    # 1. Rename CreationDate to a logical 'Observation_Time'
    if 'CreationDate' in df.columns:
        df = df.rename(columns={'CreationDate': 'Observation_Time'})
        
    # 2. Drop unnecessary system columns to make the CSV cleaner
    cols_to_drop = ['globalid', 'Creator', 'EditDate', 'Editor']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
    
    # 3. Convert timestamps to Sri Lanka Time (UTC+5:30)
    for col in df.columns:
        if 'time' in col.lower() or 'date' in col.lower():
            try:
                # Convert to UTC Datetime then add 5.5 hours for Sri Lanka time
                df[col] = pd.to_datetime(df[col], unit='ms')
                df[col] = df[col] + timedelta(hours=5, minutes=30)
                # Ensure the timezone is stripped for safe exporting
                df[col] = df[col].dt.tz_localize(None)
                print(f"🕒 Converted column '{col}' to Sri Lanka Time (UTC+5:30)")
            except Exception:
                pass 
                
    return df

# 1. Run the scraper
df = scrape_rainfall_data(API_URL)

if df is not None:
    # 2. Show a quick preview of the cleaned data
    print("\n📊 Cleaned Data Preview:")
    print(df.head()) # Note: Standard print() used instead of Colab's display()
    
    # 3. Force the grouping by 'gauge' (station name) instead of 'basin'
    target_col = 'gauge'

    if target_col in df.columns:
        print(f"\n📂 Appending new data to separate files by individual station: '{target_col}'")
        
        # 4. Create a folder to hold all the CSV files
        folder_name = "Station_Rainfall_CSVs"
        os.makedirs(folder_name, exist_ok=True)
        
        # Get all unique stations
        unique_locations = df[target_col].dropna().unique()
        
        # 5. Loop through each station and append/create a separate CSV
        for location in unique_locations:
            # Filter newly fetched data for this specific station
            location_df = df[df[target_col] == location]
            
            # Clean up the station name so it's safe to use as a file name
            safe_name = str(location).replace("/", "_").replace("\\", "_").replace(" ", "_")
            file_path = f"{folder_name}/{safe_name}.csv"
            
            # Check if this station's file already exists from a previous run
            if os.path.exists(file_path):
                # Load the existing historical data
                existing_df = pd.read_csv(file_path)
                
                # Make sure Pandas treats the existing time column as a Datetime format for safe comparison
                if 'Observation_Time' in existing_df.columns:
                    existing_df['Observation_Time'] = pd.to_datetime(existing_df['Observation_Time'])
                    
                # Combine the old data with the newly downloaded data
                combined_df = pd.concat([existing_df, location_df])
            else:
                # If the file doesn't exist yet, just use the newly downloaded data
                combined_df = location_df
                
            # Remove any duplicate rows based on the Observation_Time (keeps the newest data point)
            if 'Observation_Time' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['Observation_Time'], keep='last')
                # Sort the data chronologically so the CSV goes from oldest to newest
                combined_df = combined_df.sort_values(by='Observation_Time')
            
            # Save the seamlessly updated dataset back to the CSV file
            combined_df.to_csv(file_path, index=False)
            
        print(f"✅ Successfully updated {len(unique_locations)} station files inside the '{folder_name}' folder.")
        print(f"🚀 GitHub Actions will now automatically commit these changes to your repository!")
        
    else:
        print(f"\n⚠️ The column '{target_col}' was not found in the dataset.")

import requests
import pandas as pd
import zipfile
import io
import datetime
import os
from bs4 import BeautifulSoup
import time

# Script Configuration
STOCK_DATA_DIR = "StockData"
PKL_FILENAME = "merged_stock_data.pkl"

# List of Scrip Codes to filter
large_midcap_SC = [
    500325, 500180, 890157, 532540, 532174, 500112, 500209, 500034, 500696, 500875,
    500520, 532281, 500247, 532215, 532538, 500114, 541154, 543320, 500049, 512599,
    540376, 500820, 500790, 539448, 530965, 500188, 543940, 544274, 500300, 500295,
    505200, 500440, 540005, 541450, 540180, 532343, 500331, 500825, 500425, 544574,
    532134, 511218, 544569, 532461, 532810, 500087, 500420, 532424, 543287, 532155,
    532483, 544390, 500550, 500530, 500002, 500480, 500800, 544576, 542652, 532477,
    539254, 532286, 500116, 544285, 543904, 532814, 534816, 532754, 532955, 532321,
    544277, 544252, 500096, 500257, 544162, 503806, 500103, 543187, 544632, 544289,
    543390, 500477, 532667, 540691, 543396, 532388, 544600, 522275, 542649, 532539,
    532843, 542066, 532648, 544307, 500488, 524804, 526371, 539523, 509480, 500290,
    500368, 544429, 533519, 543994, 505790, 532187, 540611, 500830, 500493, 532541,
    512070, 503100, 532149, 540762, 541143, 539437, 532296, 500271, 500113, 500469,
    543278, 523457, 590071, 509930, 532523, 540222, 500163, 532478, 532827, 502355,
    544325, 540678, 544238, 544362, 500575, 542772, 532525, 532522, 544449, 542216,
    500495, 532497, 540153, 517569, 532830, 500411, 544609, 532505, 500067, 539551,
    500164, 524000, 500092, 523395, 524494, 544597, 532259, 543654
]

def setup_directories():
    if not os.path.exists(STOCK_DATA_DIR):
        os.makedirs(STOCK_DATA_DIR)

def download_bse_zip(current_date):
    """
    Downloads and extracts BSE data.
    URL Format: https://www.bseindia.com/BSEDATA/gross/YYYY/SCBSEALLDDMM.zip
    """
    yyyy = current_date.strftime("%Y")
    ddmm = current_date.strftime("%d%m")
    url = f"https://www.bseindia.com/BSEDATA/gross/{yyyy}/SCBSEALL{ddmm}.zip"
    
    print(f"Attempting to download BSE ZIP from: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(STOCK_DATA_DIR)
            print("BSE ZIP downloaded and extracted successfully.")
            return f"SCBSEALL{ddmm}.TXT" # Assuming the file inside uses the same naming convention or similar
    except requests.exceptions.RequestException as e:
        print(f"Error downloading BSE ZIP: {e}")
        return None
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip file.")
        return None

def download_samco_bhavcopy(current_date):
    """
    Downloads CSV from Samco via POST request.
    URL: https://www.samco.in/bse_nse_mcx/getBhavcopy
    """
    url = "https://www.samco.in/bse_nse_mcx/getBhavcopy"
    date_str = current_date.strftime("%Y-%m-%d")
    
    payload = {
        'start_date': date_str,
        'end_date': date_str,
        'bhavcopy_data[]': 'BSE',
        'show_or_down': '1'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    print(f"Requesting Samco Bhavcopy for date: {date_str}")
    
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'lxml')
        links = soup.find_all('a', class_='bhavcopy-table-body-link')
        
        downloaded_files = []
        
        if not links:
            print("No CSV links found in Samco response.")
            return []

        for link in links:
            href = link.get('href')
            if href:
                file_name = link.text.strip()
                # Ensure filename ends with .csv
                if not file_name.lower().endswith('.csv'):
                    file_name += '.csv'
                
                print(f"Found CSV link: {href}, downloading as {file_name}")
                
                csv_response = requests.get(href, headers=headers)
                csv_response.raise_for_status()
                
                file_path = os.path.join(STOCK_DATA_DIR, file_name)
                with open(file_path, 'wb') as f:
                    f.write(csv_response.content)
                
                downloaded_files.append(file_path)
                print(f"Downloaded: {file_path}")
        
        return downloaded_files

    except requests.exceptions.RequestException as e:
        print(f"Error communicating with Samco: {e}")
        return []

def merge_and_accumulate(bse_file_name, samco_files, current_date):
    if not bse_file_name:
        print("BSE file missing, skipping merge.")
        return

    bse_path = os.path.join(STOCK_DATA_DIR, bse_file_name)
    if not os.path.exists(bse_path):
        # Fallback: check if the extracted file has a generic name or slightly different case
        # For now, assuming exact match or we search dir
        # The user prompt says "SCBSEALLDDMM.TXT" is the first file name format implies inside zip it matches
        print(f"Expected BSE file not found: {bse_path}")
        return

    print(f"Reading BSE file: {bse_path}")
    try:
        # User said pipe delimited
        df_bse = pd.read_csv(bse_path, sep='|')
    except Exception as e:
        print(f"Error reading BSE file: {e}")
        return

    # Find the matching Samco file (YYYYMMDD_BSE.csv)
    # The prompt implies a specific naming convention or we look for 'BSE' in name
    # "Download this csv file and name it as the corresponding anchor text value"
    # Typically Samco BSE file anchor text is something like '05022026_BSE'
    
    samco_bse_file = None
    target_samco_name_part = f"{current_date.strftime('%d%m%Y')}_BSE" # e.g. 05022026_BSE
    # Actually user prompt says: using YYYYMMDD in one place and DDMMYYYY possibly in another example? 
    # Prompt says: "SC_CODE' in the second file (YYYYMMDD_BSE.csv)"
    # But usually links are DDMMYYYY_BSE.csv or similar. I'll search for *BSE.csv in downloaded files.
    
    for fpath in samco_files:
        if "BSE" in os.path.basename(fpath):
            samco_bse_file = fpath
            break
            
    if not samco_bse_file:
        print("Samco BSE CSV file not found among downloaded files.")
        return

    print(f"Reading Samco file: {samco_bse_file}")
    try:
        df_samco = pd.read_csv(samco_bse_file)
    except Exception as e:
        print(f"Error reading Samco file: {e}")
        return

    # Columns: 
    # BSE: 'SCRIP CODE'
    # Samco: 'SC_CODE'
    
    # Standardize column names for merge if needed, or just specify left_on/right_on
    if 'SCRIP CODE' not in df_bse.columns:
        print(f"Column 'SCRIP CODE' not found in BSE file. Columns: {df_bse.columns}")
        return
    if 'SC_CODE' not in df_samco.columns:
        print(f"Column 'SC_CODE' not found in Samco file. Columns: {df_samco.columns}")
        return

    # Merge
    print("Merging files...")
    # Using inner join to keep only matching records, or left/outer? 
    # "Now I would like to merge these 2 files into single pkl file"
    # Usually implies getting attributes from both. Inner join is safest to ensure data integrity.
    merged_df = pd.merge(df_bse, df_samco, left_on='SCRIP CODE', right_on='SC_CODE', how='inner')
    
    # Filter by SC_CODE in huge array
    print(f"Filtering for {len(large_midcap_SC)} Scrip Codes...")
    # specific large/midcap SCs
    # Ensure SCRIP CODE is numeric for comparison
    merged_df['SCRIP CODE'] = pd.to_numeric(merged_df['SCRIP CODE'], errors='coerce')
    # filtered_df = merged_df[merged_df['SCRIP CODE'].isin(large_midcap_SC)].copy()
    filtered_df = merged_df

    if filtered_df.empty:
        print("No records matched the filter criteria.")
    else:
        print(f"Filtered data has {len(filtered_df)} rows.")

    # Add Date column for tracking over accumulation
    filtered_df['Date'] = current_date.strftime("%Y-%m-%d")

    # Accumulate
    # We use CSV for robust storage and easier debugging (text-based diffs in git)
    csv_path = os.path.join(STOCK_DATA_DIR, "merged_stock_data.csv")
    pkl_path = os.path.join(STOCK_DATA_DIR, PKL_FILENAME)
    
    final_df = None
    
    # Try loading from CSV first (Primary persistence)
    if os.path.exists(csv_path):
        print(f"Loading existing CSV: {csv_path}")
        try:
            existing_df = pd.read_csv(csv_path)
            # Ensure 'SCRIP CODE' is numeric in existing data too
            if 'SCRIP CODE' in existing_df.columns:
                existing_df['SCRIP CODE'] = pd.to_numeric(existing_df['SCRIP CODE'], errors='coerce')
        except Exception as e:
            print(f"Error reading CSV, trying PKL fallback: {e}")
            existing_df = None
    
    # Fallback to PKL if CSV didn't work (Migration or legacy)
    elif os.path.exists(pkl_path):
        print(f"Loading existing PKL (Legacy): {pkl_path}")
        try:
            existing_df = pd.read_pickle(pkl_path)
        except Exception as e:
            print(f"Error reading existing PKL: {e}")
            existing_df = None
    else:
        existing_df = None

    if existing_df is not None:
        print("Appending new data to existing history...")
        final_df = pd.concat([existing_df, filtered_df], ignore_index=True)
        # Remove duplicates
        final_df.drop_duplicates(subset=['SCRIP CODE', 'Date'], keep='last', inplace=True)
    else:
        print("Starting fresh accumulation file.")
        final_df = filtered_df

    # Save to BOTH CSV and PKL
    print(f"Saving accumulated data to CSV: {csv_path}")
    final_df.to_csv(csv_path, index=False)
    
    print(f"Saving accumulated data to PKL: {pkl_path}")
    final_df.to_pickle(pkl_path)
    
    print("\n--- Accumulation File Preview (First 5 Rows) ---")
    print(final_df.head().to_string())
    print("\n--- Accumulation File Preview (Last 5 Rows) ---")
    print(final_df.tail().to_string())
    
    print("Process completed successfully.")

import argparse

def main():
    setup_directories()
    
    parser = argparse.ArgumentParser(description="Download and process stock data for a specific date.")
    parser.add_argument("--date", type=str, help="Date in YYYY-MM-DD format (default: today)")
    args = parser.parse_args()
    
    if args.date:
        try:
            now = datetime.datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")
            return
    else:
        now = datetime.datetime.now()
    
    print(f"Starting execution for date: {now.strftime('%Y-%m-%d')}")
    
    # 1. BSE
    bse_file = download_bse_zip(now)
    
    # 2. Samco
    samco_files = download_samco_bhavcopy(now)
    
    # 3. Merge & Process
    if bse_file and samco_files:
        merge_and_accumulate(bse_file, samco_files, now)
    else:
        print("Skipping merge due to missing download(s).")

if __name__ == "__main__":
    main()

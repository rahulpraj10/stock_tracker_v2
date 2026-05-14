from Fundamentals import Tickertape
ttp = Tickertape()
import pandas as pd
import json
import os
import pickle
from bs4 import BeautifulSoup
import numpy as np

print('Running MF ETL')

Fund_slug_dict = {'Quant Small Cap Fund': 'mutualfunds/quant-small-cap-fund-M_QUNF',
 'Nippon India Small Cap': 'mutualfunds/nippon-india-small-cap-fund-M_NISL',
 'Bandhan Small Cap Fund' : 'mutualfunds/bandhan-small-cap-fund-M_IDFM',
 'Invesco India Smallcap':'mutualfunds/invesco-india-smallcap-fund-M_INEP',
 'ITI Small Cap Fund': 'mutualfunds/iti-small-cap-fund-M_ITIM',
 'Motilal Oswal Midcap':'mutualfunds/motilal-oswal-midcap-fund-M_MOLS',
 'Tata Small Cap Fund' :'mutualfunds/tata-small-cap-fund-M_TASC',
 'Bank of India Small Cap': 'mutualfunds/bank-of-india-small-cap-fund-M_BOIL',
 'Union Small Cap Fund': 'mutualfunds/union-small-cap-fund-M_UNSL',
 'HSBC Small Cap Fund' : 'mutualfunds/hsbc-small-cap-fundidcw-M_LTEB',
 'Edelweiss Mid Cap Fund': 'mutualfunds/edelweiss-mid-cap-fund-M_EDME',
 'Mahindra Manulife Small Cap' : 'mutualfunds/mahindra-manulife-small-cap-fundidcw-M_MAAA',
 'Franklin India Small Cap': 'mutualfunds/franklin-india-small-cap-fund-M_FRSM',
 'Canara Robeco Small Cap' : 'mutualfunds/canara-rob-small-cap-fund-M_CANM'
 }

mf_list = ['Quant Small Cap Fund','Nippon India Small Cap','Bandhan Small Cap Fund','Invesco India Smallcap','ITI Small Cap Fund','Motilal Oswal Midcap','Tata Small Cap Fund','Bank of India Small Cap',
           'Union Small Cap Fund', 'HSBC Small Cap Fund', 'Edelweiss Mid Cap Fund', 'Mahindra Manulife Small Cap','HDFC Mid-Cap Opportunities','Franklin India Small Cap','Canara Robeco Small Cap']

# mf_list = ['Quant Small Cap Fund']

full_holding_df = pd.DataFrame()

for scheme in mf_list:
  try:
    holdings_df = pd.DataFrame()
    #_, raw_data = ttp.get_ticker(scheme, search_place='mutualfund')
    #slug_url = raw_data[0].get('slug')
    slug_url = Fund_slug_dict[scheme]
    stock_slug_endpoint=slug_url
    response = ttp.session.get(f'https://www.tickertape.in/{stock_slug_endpoint}')
    soup = BeautifulSoup(response.text, features="html5lib")
    sp_div = soup.find('script', attrs={'id': '__NEXT_DATA__'})
    data = json.loads(sp_div.contents[0].text)
    # data
    holdings_data = data.get('props').get('pageProps').get('holdingsGraph').get('currentAllocation')
    holdings_df = pd.DataFrame(holdings_data)
    holdings_df['fund'] = scheme
    holdings_df['Month_Year'] = pd.to_datetime('today').to_period('M')
    print(f'Found {len(holdings_data)} constitents stocks for MF: {scheme}')
    full_holding_df = pd.concat([full_holding_df, holdings_df])
  except Exception as e:
    print(f'Error in {scheme}: {e}')

# full_holding_df


with open('./StockData/stock_code_mapping.pkl', 'rb') as f:
    mapping_df1 = pickle.load(f)
# mapping_df1

full_holding_df1 = full_holding_df.join(mapping_df1.set_index('ticker'), on='ticker')
# full_holding_df1


directory = "./StockData"  # Change this to your folder path
f_last2last = os.path.join(directory, "last2last_month_MF_holdings.pkl")
f_last = os.path.join(directory, "last_month_MF_holdings.pkl")
f_current = os.path.join(directory, "current_month_MF_holdings.pkl")

def rotate_and_save(df):
    # Step A: Shift 'last' to 'last2last'
    # We check if the file exists first to avoid errors on the very first run
    if os.path.exists(f_last):
        os.replace(f_last, f_last2last)
        print(f"Moved {f_last} -> {f_last2last}")

    # Step B: Shift 'current' to 'last'
    if os.path.exists(f_current):
        os.replace(f_current, f_last)
        print(f"Moved {f_current} -> {f_last}")

    # Step C: Save the new dataframe as 'current'
    df.to_pickle(f_current)
    print(f"Saved new data to {f_current}")

# Execute the rotation
rotate_and_save(full_holding_df1)

# Load both the pkl files , current month and last month
# load pkl file

with open('./StockData/last_month_MF_holdings.pkl', 'rb') as f:
    last_month_holding_df = pickle.load(f)

with open('./StockData/current_month_MF_holdings.pkl', 'rb') as f:
    current_month_holding_df = pickle.load(f)

df = pd.concat([last_month_holding_df, current_month_holding_df], axis = 0)

df = df.loc[(df.type == 'Equity')]
# 1. Filter data for the two most recent periods
# Assuming df is your main dataframe
periods = sorted(df['Month_Year'].unique(), reverse=True)
current_period = periods[0]
last_period = periods[1]

df_curr = df[df['Month_Year'] == current_period]
df_last = df[df['Month_Year'] == last_period]

# 2. Merge current and last month on Stock and Fund to compare side-by-side
# We use an outer join to catch 'Added' (new in current) and 'Removed' (gone from current)
merged = pd.merge(
    df_curr[['ticker', 'fund', 'latest']],
    df_last[['ticker', 'fund', 'latest']],
    on=['ticker', 'fund'],
    how='outer',
    suffixes=('_curr', '_last')
)

# Fill NaNs with 0 to handle additions/removals mathematically
merged['latest_curr'] = merged['latest_curr'].fillna(0)
merged['latest_last'] = merged['latest_last'].fillna(0)

# 3. Define the logic for each category
conditions = [
    (merged['latest_last'] == 0) & (merged['latest_curr'] > 0),    # Added
    (merged['latest_last'] > 0) & (merged['latest_curr'] == 0),    # Removed
    (merged['latest_curr'] > merged['latest_last']) & (merged['latest_last'] > 0), # Increased
    (merged['latest_curr'] < merged['latest_last']) & (merged['latest_curr'] > 0), # Decreased
    (merged['latest_curr'] == merged['latest_last']) & (merged['latest_curr'] > 0) # No Change
]

choices = ['added', 'removed', 'increased', 'decreased', 'no_change']
merged['action'] = np.select(conditions, choices, default='none')

# --- FIX STARTS HERE ---
# Force 'action' to be categorical so all categories are tracked even if empty
all_categories = ['added', 'removed', 'increased', 'decreased', 'no_change']
merged['action'] = pd.Categorical(merged['action'], categories=all_categories)

# 4. Group by Stock and Pivot (observed=False ensures 0-count categories stay)
analysis = merged.groupby(['ticker'], observed=False)['action'].value_counts().unstack(fill_value=0)
# --- FIX ENDS HERE ---

analysis['num MF holding'] = merged[merged['latest_curr'] > 0].groupby(['ticker'])['fund'].count()

# 6. Cleanup and Rename columns to match your request
analysis = analysis.rename(columns={
    'added': 'mf_Added',
    'removed': 'MF_removed',
    'increased': 'mf_increased_investment',
    'decreased': 'mf_decreased_investment',
    'no_change': 'mf_no_change_investment'
}).reset_index()

final_cols = [
    'ticker', 'num MF holding', 'mf_Added', 'MF_removed',
    'mf_increased_investment', 'mf_decreased_investment', 'mf_no_change_investment'
]
analysis = analysis[final_cols]

with open('./StockData/stock_code_mapping.pkl', 'rb') as f:
    mapping_df1 = pickle.load(f)
# mapping_df1

analysis = analysis.join(mapping_df1.set_index('ticker'), on='ticker')
analysis['SC Code'] = analysis['SC Code'].fillna(0)
analysis['SC Code'] = analysis['SC Code'].astype(int)
print(analysis)

directory = "./StockData"
f_analysis_current_file = os.path.join(directory, "MF_holdings_analysis.pkl")
analysis.to_pickle(f_analysis_current_file)




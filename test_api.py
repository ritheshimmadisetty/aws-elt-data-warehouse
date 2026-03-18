import requests
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('ALPHA_VANTAGE_API_KEY')

# Fetch daily stock data for Infosys
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=INFY&outputsize=compact&apikey={api_key}"

response = requests.get(url)
data = response.json()

# Print first 3 days of data
time_series = data.get('Time Series (Daily)', {})
for i, (date, values) in enumerate(time_series.items()):
    if i >= 3:
        break
    print(f"Date: {date}")
    print(f"  Open:   {values['1. open']}")
    print(f"  High:   {values['2. high']}")
    print(f"  Low:    {values['3. low']}")
    print(f"  Close:  {values['4. close']}")
    print(f"  Volume: {values['5. volume']}")
    print()
from polygon import RESTClient
from google.cloud import bigquery
from datetime import datetime
import datetime as dt
import pytz

POLYGON_API_KEY = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
client = RESTClient(api_key=POLYGON_API_KEY)

service_account_path = '../creds.json'
bq_client = bigquery.Client.from_service_account_json(service_account_path)

project_id = 'bigdata-421623'
dataset = 'ForEx_Big_Data'
project = 'BigData'
table = 'Minute_Forex'
table_ref = bq_client.dataset(dataset).table(table)

tickers = ['C:EURUSD', 'C:USDJPY', 'C:GBPUSD', 'C:JPYEUR', 'C:USDEUR',
           'C:JPYUSD', 'C:USDGBP', 'C:EURJPY', 'C:JPYGBP', 'C:GBPJPY',
           'C:GBPEUR', 'C:EURGBP']


def format_row(agg_obj, ticker):

    time_value = datetime.fromtimestamp(agg_obj.timestamp / 1000, tz=pytz.UTC).isoformat()
    created_at_value = datetime.utcnow().isoformat()

    return {
        'opening_price': agg_obj.open,
        'highest_price': agg_obj.high,
        'lowest_price': agg_obj.low,
        'closing_price': agg_obj.close,
        'volume': agg_obj.volume,
        'avg_volume_weight': agg_obj.vwap,
        'transactions': agg_obj.transactions,
        'time': time_value,
        'created_at': created_at_value,
        'ticker': ticker  # You may need to adjust this based on your actual data
    }


def get_data_from_polygon(ticker):
    # Current time and 15 minutes ago
    now = datetime.now()
    fifteen_minutes_ago = now - dt.timedelta(hours=1)

    # Today's date formatted for API call
    today_date = now.strftime("%Y-%m-%d")

    # Fetch today's data
    aggs = []
    for a in client.list_aggs(ticker=ticker, multiplier=1, timespan="hour", from_=today_date, to=today_date, limit=5000):
        aggs.append(a)

    # Filter to keep only last 5 minutes data
    filtered_aggs = [agg for agg in aggs if datetime.fromtimestamp(agg.timestamp / 1000) >= fifteen_minutes_ago]

    return filtered_aggs


def get_data_from_bq(query):
    query = f"SELECT * FROM `{dataset}.{table}`"
    query_job = client.query(query)
    print(query_job)

    # Print the results
    for row in query_job:
        print(dict(row))


def insert_data_in_bq(rows_to_insert, table_ref):
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print('Errors occurred while inserting rows:', errors)
    else:
        print('Rows have been successfully inserted.')

for t in tickers:
    data = get_data_from_polygon(ticker=t)
    rows = [format_row(agg, ticker=t) for agg in data]
    insert_data_in_bq(rows, table_ref)



from pyspark.sql import SparkSession

# Initialize Spark session with BigQuery connector
spark = SparkSession.builder.config(
    "temporaryGcsBucket", "dataproc-temp-bucket-2023"
).getOrCreate()

service_account_path = 'creds.json'
bq_client = bigquery.Client.from_service_account_json(service_account_path)

def format_row(agg_obj, ticker):

    time_value = datetime.fromtimestamp(agg_obj.timestamp / 1000, tz=pytz.UTC).isoformat()
    created_at_value = datetime.utcnow().isoformat()

    return {
        'time': time_value,
        'ticker_1': ticker,  # You may need to adjust this based on your actual data
        'ticker_2': ticker,
        'ticker_3': ticker,
        'arbitrage_value' : arbitrage value,
        'created_at': created_at_value
    }

def insert_data_in_bq(rows_to_insert, table_ref):
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print('Errors occurred while inserting rows:', errors)
    else:
        print('Rows have been successfully inserted.')


from itertools import permutations

# Function to compute arbitrage for a triplet and return conversion flow
def compute_arbitrage_with_flow(base, quote1, quote2, df):
    df1 = df.filter(df.ticker == f'C:{base}{quote1}')
    df2 = df.filter(df.ticker == f'C:{quote1}{quote2}')
    df3 = df.filter(df.ticker == f'C:{quote2}{base}')
    
    if df1.count() == 0 or df2.count() == 0 or df3.count() == 0:
        return float('-inf'), []

    rate1 = df1.agg({'closing_price': 'avg'}).collect()[0][0]
    rate2 = df2.agg({'closing_price': 'avg'}).collect()[0][0]
    rate3 = df3.agg({'closing_price': 'avg'}).collect()[0][0]

    arbitrage_value = rate1 * rate2 * rate3

    conversion_flow = [
        f"{base} -> {quote1} @ {rate1}",
        f"{quote1} -> {quote2} @ {rate2}",
        f"{quote2} -> {base} @ {rate3}"
    ]

    return arbitrage_value, conversion_flow

# Get all possible triplet combinations
triplets = permutations(currencies, 3)

# Track unique sets and filter results
unique_triplets = set()
arbitrage_info = []
for triplet in triplets:
    sorted_triplet = tuple(sorted(triplet))
    if sorted_triplet not in unique_triplets:
        unique_triplets.add(sorted_triplet)
        arbitrage_info.append((triplet, *compute_arbitrage_with_flow(*triplet, df)))

top_three_arbitrage = sorted(arbitrage_info, key=lambda x: x[1], reverse=True)[:3]

print("Top 3 Triplets with highest arbitrage values:")
for triplet, value, flow in top_three_arbitrage:
    print(f"\nTriplet: {triplet}")
    print(f"Arbitrage Value: {value}")
    print("Conversion Flow:")
    for step in flow:
        print(f" - {step}")

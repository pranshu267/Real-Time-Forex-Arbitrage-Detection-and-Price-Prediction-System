from google.cloud import bigquery
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, first
from itertools import permutations
import pandas as pd

spark = SparkSession.builder.config(
    "temporaryGcsBucket", "dataproc-temp-bucket-2023"
).getOrCreate()

read_table = "bigdata-421623.ForEx_Big_Data.Hourly_Forex_copy"
write_table = "bigdata-421623.ForEx_Big_Data.Arbitrage"


def read_data():
    # Calculate the time range for the last hour
    end_time = datetime.now()
    start_time = end_time - timedelta(days=2)

    # Format timestamps for filtering
    time_filter = f"time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND time < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"

    df = spark.read.format("bigquery").option("table", read_table).load()
    return df


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


def calculate_arbitrage(df):
    # Assuming tickers are in a format like 'C:EURUSD', let's extract the unique tickers
    tickers = df.select("ticker").distinct().rdd.flatMap(lambda x: x).collect()

    # Parse out unique currencies from tickers
    currencies = set()
    for ticker in tickers:
        base, quote = ticker.split("C:")[1][:3], ticker.split("C:")[1][3:]
        currencies.update([base, quote])

    # Identify valid triangles
    currency_triangles = []
    for base in currencies:
        for via in currencies:
            for quote in currencies:
                if base != via and via != quote and base != quote:
                    ticker1 = f"C:{base}{via}"
                    ticker2 = f"C:{via}{quote}"
                    ticker3 = f"C:{quote}{base}"
                    if all(t in tickers for t in [ticker1, ticker2, ticker3]):
                        currency_triangles.append((ticker1, ticker2, ticker3))


    # Define a threshold for arbitrage opportunities
    threshold = 0.01

    # Check each triangle for arbitrage opportunities
    for triangle in currency_triangles:
        ticker1, ticker2, ticker3 = triangle

        # Pivot the DataFrame to align closing prices by ticker and time
        pivot_df = df.filter(
            df["ticker"].isin(ticker1, ticker2, ticker3)
        ).groupBy("time").pivot("ticker").agg(first("closing_price").alias("closing_price"))

        # Calculate arbitrage opportunity result including the threshold
        arb_df = pivot_df.withColumn(
            "arbitrage_result",
            when(
                (col(ticker1) * col(ticker2) / col(ticker3) - 1) > threshold, "Opportunity"
            ).otherwise("None")
        )

        # Filter for rows where an opportunity is found and display them
        arb_df.filter(col("arbitrage_result") == "Opportunity").show()

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

    return top_three_arbitrage


def write_to_bq(tt):
    service_account_path = 'creds.json'
    bq_client = bigquery.Client.from_service_account_json(service_account_path)
    project_id = 'bigdata-421623'
    dataset = 'ForEx_Big_Data'
    project = 'BigData'
    table = 'Arbitrage'

    t1 = []
    t2 = []
    t3 = []
    v = []

    for i in tt:
        t1.append(i[0][0])
        t2.append(i[0][1])
        t3.append(i[0][2])
        v.append(i[1])

    adf = pd.DataFrame(
        {'ticker1': t1,
         'ticker2': t2,
         'ticker3': t3,
         'arbitrage_value': v
         })

    created_at_value = datetime.utcnow().isoformat()
    adf['created_at'] = created_at_value

    adf.write.format("bigquery").option(
        "table", write_table
    ).mode("append").save()



df = read_data()
tt = calculate_arbitrage(df)
write_to_bq(tt)
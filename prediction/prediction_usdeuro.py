from google.cloud import bigquery
import torch
from datetime import datetime
from lstm import BidirectionalLSTM
import numpy as np
from prefect import flow, task, get_run_logger

model = BidirectionalLSTM(input_dim=6, hidden_dim=50, num_layers=2, output_dim=3, dropout_rate=0.2)
model.load_state_dict(torch.load('trained_models/usdeur_model_state_dict.pth'))
model.eval()  

service_account_path = 'creds.json'
client = bigquery.Client.from_service_account_json(service_account_path)

project_id = 'bigdata-421623'
dataset = 'ForEx_Big_Data'
project = 'BigData'
table = 'Hourly_Forex_copy'
table_write = 'pred'
table_ref = client.dataset(dataset).table(table_write)

ticker = "C:USDEUR" 
mean_price = 0.92345
mean_volume = 3704.2965
mean_avg_volume = 0.92371

std_dev_price = 0.012939
std_dev_volume = 2259.80263
std_dev_avg_volume = 0.012947

@task(
    description="format rows",
    log_prints=True
)
def get_latest_data(ticker):
    query = f"""
    SELECT closing_price, highest_price, lowest_price, opening_price, volume, avg_volume_weight
    FROM `{dataset}.{table}`
    WHERE ticker = '{ticker}'
    ORDER BY time DESC
    LIMIT 1
    """
    query_job = client.query(query)  
    for row in query_job:
        return dict(row)


@task(
    description="format rows",
    log_prints=True
)
def write_prediction_to_bigquery(ticker, prediction):
    
    scaled_prediction = (prediction*std_dev_price) + mean_price
    rows_to_insert = [
        {"ticker": ticker, "price_1": scaled_prediction[0][0].item(), "price_2": scaled_prediction[0][1].item(), "price_3": scaled_prediction[0][2].item(), "prediction_time": datetime.utcnow().isoformat()}
    ]
    print(rows_to_insert)
    errors = client.insert_rows_json(table_ref, rows_to_insert)  
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


@flow(
    description="main flow"
)
def prediction_main():
    latest_data = get_latest_data(ticker)

    scaled_closing_price = (latest_data['closing_price']*std_dev_price) + mean_price
    scaled_highest_price = (latest_data['highest_price']*std_dev_price) + mean_price
    scaled_lowest_price = (latest_data['lowest_price']*std_dev_price) + mean_price
    scaled_opening_price = (latest_data['opening_price']*std_dev_price) + mean_price
    scaled_volume_price = (latest_data['volume']*std_dev_volume) + mean_volume
    scaled_avg_volume_weight_price = (latest_data['avg_volume_weight']*std_dev_avg_volume) + mean_avg_volume

    features = np.array([[scaled_closing_price, scaled_highest_price, scaled_lowest_price, scaled_opening_price, scaled_volume_price, scaled_avg_volume_weight_price]])
    features = features.reshape(1, 1, 6)
    data_tensor = torch.tensor(features, dtype=torch.float32)

    with torch.no_grad():
        prediction = model(data_tensor)

    write_prediction_to_bigquery(ticker, prediction)


if __name__ == "__main__":
    prediction_main()

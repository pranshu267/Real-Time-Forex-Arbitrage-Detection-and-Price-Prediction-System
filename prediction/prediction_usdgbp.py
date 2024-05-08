from google.cloud import bigquery
import torch
from datetime import datetime
from lstm import BidirectionalLSTM
import numpy as np
from prefect import flow, task, get_run_logger

model = BidirectionalLSTM(input_dim=6, hidden_dim=50, num_layers=2, output_dim=3, dropout_rate=0.2)
model.load_state_dict(torch.load('trained_models/usdgbp_model_state_dict.pth'))
model.eval()  

service_account_path = 'creds.json'
client = bigquery.Client.from_service_account_json(service_account_path)

project_id = 'bigdata-421623'
dataset = 'ForEx_Big_Data'
project = 'BigData'
table = 'Hourly_Forex_copy'
table_write = 'hourly_prediction'
table_ref = client.dataset(dataset).table(table_write)

ticker = "C:USDGBP" 
mean_price = 0.798409
mean_volume = 3291.9162
mean_avg_volume = 0.79849

std_dev_price = 0.014886
std_dev_volume = 2410.08784
std_dev_avg_volume = 0.014842

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
        {"ticker": ticker, "prediction_price_t+1": str(scaled_prediction.numpy()[0][0]), "prediction_price_t+2": str(scaled_prediction.numpy()[0][1]), "prediction_price_t+3": str(scaled_prediction.numpy()[0][2]), "prediction_time": datetime.utcnow().isoformat()}
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
def prediction_main_2():
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
    prediction_main_2()
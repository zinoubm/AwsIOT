import pandas as pd
import boto3
import json
from tqdm import tqdm

DELIVERY_STREAM_NAME = "IotStream"


def send_batch_to_firehose(batch):
    records = [
        {"Data": json.dumps(record, indent=2).encode("utf-8") + b"\n"}
        for record in batch
    ]
    response = firehose_client.put_record_batch(
        DeliveryStreamName=DELIVERY_STREAM_NAME,
        Records=records,
    )

    return response


df = pd.read_csv("data/IOT-temp.csv")
firehose_client = boto3.client("firehose")

batch_size = 128
batches = []

for index, row in tqdm(df.iterrows(), total=len(df)):
    record = row.to_dict()
    batches.append(record)

    if len(batches) == batch_size:
        response = send_batch_to_firehose(batches)
        batches = []

print("Put Records Succeeded!")

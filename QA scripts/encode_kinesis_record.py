import json
import base64
import sys

# Usage: python encode_kinesis_record.py input.json
# input.json should contain a JSON object with 'data' and 'metadata' keys


import random
import time


def make_kinesis_record(encoded_data, partition_key):
    return {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": partition_key,
            "sequenceNumber": str(random.randint(10**50, 10**51-1)),
            "data": encoded_data,
            "approximateArrivalTimestamp": time.time()
        },
        "eventSource": "aws:kinesis",
        "eventVersion": "1.0",
        "eventID": f"shardId-000000000000:{random.randint(10**50, 10**51-1)}",
        "eventName": "aws:kinesis:record",
        "invokeIdentityArn": "arn:aws:iam::123456789012:role/ExampleRole",
        "awsRegion": "us-east-2",
        "eventSourceARN": "arn:aws:kinesis:us-east-2:123456789012:stream/example-stream"
    }


def encode_file(input_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        records = json.load(f)
    # Support both a single object and a list of objects
    if isinstance(records, dict):
        records = [records]
    kinesis_records = []
    for rec in records:
        as_str = json.dumps(rec, ensure_ascii=False)
        encoded = base64.b64encode(as_str.encode('utf-8')).decode('utf-8')
        # Use table-name from metadata as partitionKey if available, else default
        partition_key = rec.get('metadata', {}).get('table-name', 'default')
        kinesis_records.append(make_kinesis_record(encoded, f"public.{partition_key}"))
    event = {"Records": kinesis_records}
    output_path = "QA scripts/output_test.json"
    with open(output_path, "w", encoding="utf-8") as out_f:
        json.dump(event, out_f, indent=2, ensure_ascii=False)
    print(f"Kinesis event written to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python encode_kinesis_record.py input.json")
        sys.exit(1)
    encode_file(sys.argv[1])

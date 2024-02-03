import json
import requests
import os
from tqdm import tqdm


HOST = os.environ["OPENSEARCH_HOST"]
INDEX_NAME = "spotify-index"
DOC_TYPE = "_doc"  # For OpenSearch 7.x and later, use "_doc"
FILE_PATH = "data.json"
USERNAME = os.environ["OPENSEARCH_USERNAME"]
PASSWORD = os.environ["OPENSEARCH_PASSWORD"]
BATCH_SIZE = 5000

HEADERS = {"Content-Type": "application/x-ndjson"}

AUTH = (USERNAME, PASSWORD)


def prepare_bulk_payload(records):
    """Prepare bulk payload from list of records."""
    bulk_data = []
    for record in records:
        index_action = {"index": {"_index": INDEX_NAME}}
        bulk_data.append(json.dumps(index_action))
        bulk_data.append(json.dumps(record))

    return "\n".join(bulk_data) + "\n"


# Load the data from the JSON file
with open(FILE_PATH, "r") as file:
    data = json.load(file)

# Split data into batches of 5,000 and push to OpenSearch
for index in tqdm(range(0, len(data), BATCH_SIZE)):
    batch = data[index: index + BATCH_SIZE]
    bulk_body = prepare_bulk_payload(batch)

    url = f"{HOST}/_bulk"
    response = requests.post(url, headers=HEADERS, data=bulk_body, auth=AUTH, verify=False)

    # Optional: print the response to check for any issues
    print(response.json())

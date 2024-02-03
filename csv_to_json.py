import pandas as pd
import json

csv_file = "data.csv"
json_file = "data.json"

df = pd.read_csv(csv_file)
df.to_json(json_file, orient="records")

# Databricks notebook source
json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"


# COMMAND ----------
#Split wikipedia clickstream dataset in multiple json to simulate incremental data 

import json
import time

json_file = open(json_path)
SIZE = 100
SLEEP = 10
OFFSET=0
lines = []
i = 0
for line in json_file:
    lines.append(line)
    if len(lines) == SIZE:
        json_string = "["+ ",".join(lines) + "]"
        f = open(f"/dbfs/mnt/data/wiki-clickstream/clickstream_{i}.json","w")
        f.write(json_string)
        i += 1
        lines = []
        if i > OFFSET:
            time.sleep(SLEEP)
            print(f"Writing clickstream_{i}.json")


# COMMAND ----------



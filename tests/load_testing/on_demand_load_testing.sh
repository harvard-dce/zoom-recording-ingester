#!/usr/bin/env bash

# feed this script a csv with "recording url", "series id"

set -e

CSV_FILE=$1

if [[ -z "$ON_DEMAND_ENDPOINT" ]]; then
  echo '$ON_DEMAND_ENDPOINT not defined!'
  exit 1;
fi

while IFS=, read -r meeting_uuid oc_series_id
do
  payload="{\"uuid\":\"$meeting_uuid\",\"oc_series_id\":\"$oc_series_id\",\"allow_multiple_ingests\":true}"

  curl -XPOST \
  -H "Accept: application/json" \
  -H "Content-Type:application/json" \
  -d "$payload" \
  $ON_DEMAND_ENDPOINT

  echo
  sleep 30
done < $CSV_FILE



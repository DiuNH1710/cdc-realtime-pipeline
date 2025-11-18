#!/bin/bash
DEBEZIUM_URL="http://localhost:8083/connectors"

create_connector() {
  local connector_name=$1
  local table_list=$2

  curl -s -X POST -H "Content-Type: application/json" $DEBEZIUM_URL --data "{
    \"name\": \"$connector_name\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"tasks.max\": \"1\",
      \"plugin.name\": \"pgoutput\",
      \"database.hostname\": \"postgres\",
      \"database.port\": \"5432\",
      \"database.user\": \"debezium\",
      \"database.password\": \"debezium\",
      \"database.dbname\": \"etl_db\",
      \"database.server.name\": \"cdc\",
      \"publication.name\": \"dbz_pub\",
      \"slot.name\": \"cdc_slot\",
      \"table.include.list\": \"$table_list\",
      \"tombstones.on.delete\": \"false\",
      \"decimal.handling.mode\": \"string\"
    }
  }"
  echo -e "\nConnector $connector_name creation requested."
}

# Gọi: truyền danh sách bảng, ví dụ "public.tracking_events,public.search_by_jobid"
create_connector "cdc_tracking_conn" "public.tracking_events,public.search_by_jobid"

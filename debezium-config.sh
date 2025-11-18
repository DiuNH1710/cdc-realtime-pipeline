#!/bin/bash

# Debezium REST API
DEBEZIUM_URL="http://localhost:8083/connectors"

# Function to create connector for a table
create_connector() {
  local connector_name=$1
  local table_name=$2

  curl -X POST -H "Content-Type: application/json" $DEBEZIUM_URL --data "{
    \"name\": \"$connector_name\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"plugin.name\": \"pgoutput\",
      \"database.hostname\": \"postgres\",
      \"database.port\": \"5432\",
      \"database.user\": \"postgres\",
      \"database.password\": \"postgres\",
      \"database.dbname\": \"ecommerce\",
      \"database.server.name\": \"postgres\",
      \"table.include.list\": \"public.$table_name\",
      \"topic.prefix\": \"cdc\",
      \"decimal.handling.mode\": \"string\"
    }
  }"
  echo -e "\nConnector $connector_name created for table $table_name"
}


# 1. Sale report
#create_connector "sale_report_conn" "sale_report"


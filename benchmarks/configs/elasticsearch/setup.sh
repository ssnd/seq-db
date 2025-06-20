curl -X PUT "http://elastic:9200/logs-index/" \
  -H 'Content-Type: application/json' \
  -d '{
        "settings": {
          "codec": "best_compression",
          "number_of_replicas": 0,
          "number_of_shards": 3,
        },
        "mappings": {
          "dynamic": "false",
          "properties": {
            "clientip": {
              "type": "keyword"
            },
            "request": {
              "type": "text"
            },
            "size": {
              "type": "keyword"
            },
            "status": {
              "type": "keyword"
            }
          }
        }
      }'

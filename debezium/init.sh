echo "Init connector"
curl -s -S -XPOST -H Accept:application/json -H Content-Type:application/json http://localhost:8084/connectors/ -d @crm-connector.json
curl http://localhost:8084/connectors/crm-connector/status
./bin/connect-distributed conf/connect-distributed-local.properties >> KConnect_$(date '+%Y%m%d').log 2>&1 &


curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors -d '{
    "name": "web-link-connector",
    "config": {
        "connector.class": "com.sampleKconnectors.examples.MySourceConnector",
        "tasks.max": 1,
        "uri.to.analyze": "https://kafka.apache.org/",
        "output.topic.name": "kafka-output-topic-weblink"
    }
}'
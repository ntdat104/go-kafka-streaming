# Go-kafka-streaming

- docker-compose up -d
- docker exec kafka kafka-topics \
  --create --topic streaming-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

- CONSUMER_ID=1 go run consumer/consumer.go
CONSUMER_ID=2 go run consumer/consumer.go
CONSUMER_ID=3 go run consumer/consumer.go
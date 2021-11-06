create-example-topic:
	docker-compose exec kafka-1 /usr/bin/kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic teste --partitions 3 --replication-factor 1

delete-example-topic:
	docker-compose exec kafka-1 /usr/bin/kafka-topics --bootstrap-server 127.0.0.1:9092 --delete --topic teste

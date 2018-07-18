docker-compose up -d
Start-Sleep -s 5
docker-compose exec kafka kafka-topics --create --topic advisor --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
docker-compose exec kafka kafka-topics --create --topic testareno --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
docker-compose exec kafka kafka-topics --create --topic uploadvalidation --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
docker-compose exec kafka kafka-topics --create --topic available --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
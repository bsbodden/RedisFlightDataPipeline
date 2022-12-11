docker build -f ./docker/Dockerfile.airflow . -t bsb-airflow
docker build -f ./docker/Dockerfile.spark . -t spark-redis
docker network create flight-data-pipeline-net
# rm -rf ./data/redis-data/appendonlydir
# rm data/redis-data/dump.rdb
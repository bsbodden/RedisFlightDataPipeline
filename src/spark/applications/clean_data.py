import os
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from webdav4.client import Client
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def parse_arguments():
    prog = "OpenSky AVRO Data Scrubber"
    desc = "Reads state vector sample data sets from Redis and cleans the data."

    parser = argparse.ArgumentParser(prog=prog, description=desc)

    parser.add_argument(
        "--redis_host",
        "-r",
        required=False,
        default="redis-flights-redis-1",
        help="Redis HOST",
    )

    parser.add_argument(
        "--redis_port",
        "-p",
        required=False,
        default="6379",
        help="Redis PORT",
    )

    parser.add_argument(
        "--name",
        "-n",
        required=False,
        default=parser.prog,
        help="Name to use in Spark Session",
    )

    parsed_args = parser.parse_args()
    return parsed_args


def clean_data(redis_host, redis_port, name):
    LOGGER.info(">>>> Cleaning the data")
    # spark = (
    #     SparkSession.builder.appName(name)
    #     .config("spark.redis.host", redis_host)
    #     .config("spark.redis.port", redis_port)
    #     .getOrCreate()
    # )

    # df = (
    #     spark.read.format("org.apache.spark.sql.redis")
    #     .option("table", "state_vectors")
    #     .load()
    # )

    # if df.distinct().count() == 0:
    #     # Data Cleaning
    #     df.na.drop(subset=["icao24"])
    #     # Save Cleaned Data
    #     df.write.format("org.apache.spark.sql.redis").option(
    #         "table", "state_vectors"
    #     ).save()


def main():
    parsed_args = parse_arguments()
    redis_host = parsed_args.redis_host
    redis_port = parsed_args.redis_port
    name = parsed_args.name
    clean_data(redis_host, redis_port, name)


if __name__ == "__main__":
    main()

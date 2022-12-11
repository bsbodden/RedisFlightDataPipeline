import os
import argparse
import logging
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def parse_arguments():
    prog = "OpenSky AVRO Processor"
    desc = "Reads state vector sample data sets from OpenSky in AVRO format."

    parser = argparse.ArgumentParser(prog=prog, description=desc)

    parser.add_argument(
        "--avro_input_file",
        "-i",
        required=True,
        help="State Vector sample data sets in AVRO format",
    )

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


def import_avro_to_redis(avro_input_file, redis_host, redis_port, name):
    LOGGER.info(">>>> Converting and importing AVRO to Redis Hashes")
    spark = (
        SparkSession.builder.appName(name)
        .config("spark.redis.host", redis_host)
        .config("spark.redis.port", redis_port)
        .getOrCreate()
    )

    # df = (
    #     spark.read.format("org.apache.spark.sql.redis")
    #     .option("table", "state_vectors")
    #     .option("infer.schema", True)
    #     .load()
    # )

    # if df.distinct().count() == 0:

    avro_file = avro_input_file
    if avro_file.startswith(("http://", "https://")):
        LOGGER.info(f"Target URL: {avro_file}")
        url_parts = urlparse(avro_input_file)
        webdav_server = f"{url_parts.scheme}://{url_parts.netloc}"
        LOGGER.info(f"WEBDAV Server: {webdav_server}")
        webdav_path = url_parts.path
        LOGGER.info(f"WEBDAV Resource Path: {webdav_path}")
        # file is in webdav server
        client = Client(webdav_server)
        # generate a random file name for local storage
        avro_file = f"/tmp/{os.path.basename(webdav_path)}.avro"
        LOGGER.info(f"Downloading to temp file: {avro_file}")
        # copy file to local
        client.download_file(webdav_path, avro_file)
        LOGGER.info(f"Finished downloading to temp file: {avro_file}")

    # Read AVRO file into a Spark DataFrame
    df = spark.read.format("avro").load(avro_file)

    LOGGER.info(
        f"Load state vectors data frame with {df.distinct().count()} entries"
    )

    # sample = df.head(100)
    # sample_df = spark.createDataFrame(data=sample, schema=df.schema)

    # Write to Redis as Hashes
    df.write.format("org.apache.spark.sql.redis").option(
        "table", "state_vectors"
    ).save()


def main():
    parsed_args = parse_arguments()
    avro_input_file = parsed_args.avro_input_file
    redis_host = parsed_args.redis_host
    redis_port = parsed_args.redis_port
    name = parsed_args.name
    import_avro_to_redis(avro_input_file, redis_host, redis_port, name)


if __name__ == "__main__":
    main()

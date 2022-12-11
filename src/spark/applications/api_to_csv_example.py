import requests

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DataExtraction").getOrCreate()

# "fund_house": "Franklin Templeton Mutual Fund",
# "scheme_type": "Open Ended Schemes",
# "scheme_category": "Other Scheme - FoF Overseas",
# "scheme_code": 118550,
# "scheme_name": "Franklin India Feeder - Franklin U S Opportunities Fund - Direct - IDCW "
response = requests.get("https://api.mfapi.in/mf/118550")
data = response.text
sparkContext = spark.sparkContext
RDD = sparkContext.parallelize([data])
raw_json_dataframe = spark.read.json(RDD)

raw_json_dataframe.printSchema()
raw_json_dataframe.createOrReplaceTempView("mutual_benefit")

dataframe = (
    raw_json_dataframe.withColumn("data", F.explode(F.col("data")))
    .withColumn("meta", F.expr("meta"))
    .select("data.*", "meta.*")
)

dataframe.show(100, False)
dataframe.toPandas().to_csv("franklin_templeton_mf_oes.csv")

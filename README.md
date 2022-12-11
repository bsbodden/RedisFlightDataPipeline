# Final Project for CSCI-E88 - Principles of Big Data

WIP --

## Setup

```
./prepare-docker.sh
```

```
docker compose up
```

```
create-airflow-connections.sh
```

```
cd seaweedfs
docker compose up
```

AWS

```
aws configure
AWS Access Key ID [****************any]: yGPULekGEmxyfLaD
AWS Secret Access Key [****************any]: 1zQYqLBbpyahrtMSFCnW7ZeJ74GhmndO
Default region name [us-east-1]:
Default output format [None]:

➜ aws --endpoint-url http://localhost:8333 s3 ls
➜ aws --endpoint-url http://localhost:8333 s3 mb s3://opensky
make_bucket: opensky

➜ aws --endpoint-url http://localhost:8333 s3 ls
2022-12-08 10:48:55 opensky

➜ aws --endpoint-url http://localhost:8333 s3 cp ./data/states_2020-05-25-00.avro/states_2020-05-25-00.avro s3://opensky
upload: data/states_2020-05-25-00.avro/states_2020-05-25-00.avro to s3://opensky/states_2020-05-25-00.avro

➜ aws --endpoint-url http://localhost:8333 s3 ls opensky
2022-12-08 10:55:41   42857835 states_2020-05-25-00.avro

➜ aws --endpoint-url http://localhost:8333 s3 presign s3://opensky/states_2020-05-25-00.avro
```

Spark Submit Test

```bash
# test with localfiles
spark-submit --jars src/spark/jars/spark-redis_2.12-3.1.0-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.12:3.3.1 src/spark/applications/opensky_state_vectors_to_redis.py --avro_input_file data/states_2020-05-25-00.avro/states_2020-05-25-00.avro --redis_host 127.0.0.1

# test with webdav endpoint
➜ spark-submit --jars src/spark/jars/spark-redis_2.12-3.1.0-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.12:3.3.1 src/spark/applications/opensky_state_vectors_to_redis.py --avro_input_file http://127.0.0.1:7333/buckets/opensky/states_2020-05-25-01.avro --redis_host 127.0.0.1
```

====

# Batch Collection

Load data from Avro files to be transformed by the "Flight Builder" into
"Flight" structures to be store as Redis Hashes

# Notes

```sql
select sv.icao24, sv.callsign,
sv.lat as lat2, sv.lon as lon2, sv.baroaltitude as alt2, sv.hour,
est.firstseen, est.estdepartureairport, est.lastseen, est.estarrivalairport,
est.day, est.lat1, est.lon1, est.alt1
from state_vectors_data4 as sv join (
    select icao24 as e_icao24, firstseen, lastseen,
    estdepartureairport, estarrivalairport, callsign as e_callsign, day,
    t.time as t1, t.latitude as lat1, t.longitude as lon1, t.altitude as alt1
    from flights_data4, flights_data4.track as t
    where ({before_hour} <= day and day <= {after_hour}) and t.time = firstseen
) as est
on sv.icao24 = est.e_icao24 and sv.callsign = est.e_callsign and
est.lastseen = sv.time
where hour>={before_hour} and hour<{after_hour} and
time>={before_time} and time<{after_time}
```

```sql
CREATE TABLE flights(
et bigint,
icao24 varchar(20), ✅
lat float, ✅
lon float,✅
velocity float,✅
heading float,✅
vertrate float,✅
callsign varchar(10),✅
onground boolean,✅
alert boolean,✅
spi boolean,✅
squawk integer,✅
baroaltitude numeric(7,2),✅
geoaltitude numeric(7,2),✅
lastposupdate numeric(13,3),✅
lastcontact numeric(13,3)✅
);
```

```
>>> df.printSchema()
root
 |-- alert: boolean (nullable = true)
 |-- baroaltitude: double (nullable = true)
 |-- callsign: string (nullable = true)
 |-- geoaltitude: double (nullable = true)
 |-- heading: double (nullable = true)
 |-- icao24: string (nullable = true)
 |-- lastcontact: double (nullable = true)
 |-- lastposupdate: double (nullable = true)
 |-- lat: double (nullable = true)
 |-- lon: double (nullable = true)
 |-- onground: boolean (nullable = true)
 |-- spi: boolean (nullable = true)
 |-- squawk: string (nullable = true)
 |-- time: integer (nullable = true)
 |-- velocity: double (nullable = true)
 |-- vertrate: double (nullable = true)
```

```
  spark-submit --jars lib/spark-redis_2.12-3.1.0-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1 collection/batch/minio_test.py
  pyspark --jars lib/spark-redis_2.12-3.1.0-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1
➜ pyspark --jars lib/spark-redis_2.12-3.1.0-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.12:3.3.1

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.3.1
      /_/

Python 3.10.8 (main, Oct 13 2022, 10:17:43) [Clang 14.0.0 (clang-1400.0.29.102)] on darwin

>>> df = spark.read.format("avro").load("data/states_2020-05-25-00.avro/states_2020-05-25-00.avro")
>>> df.dtypes
[('alert', 'boolean'), ('baroaltitude', 'double'), ('callsign', 'string'), ('geoaltitude', 'double'), ('heading', 'double'), ('icao24', 'string'), ('lastcontact', 'double'), ('lastposupdate', 'double'), ('lat', 'double'), ('lon', 'double'), ('onground', 'boolean'), ('spi', 'boolean'), ('squawk', 'string'), ('time', 'int'), ('velocity', 'double'), ('vertrate', 'double')]
>>> df.show(2)
+-----+-----------------+--------+-----------------+------------------+------+----------------+----------------+------------------+-------------------+--------+-----+------+----------+------------------+--------+
|alert|     baroaltitude|callsign|      geoaltitude|           heading|icao24|     lastcontact|   lastposupdate|               lat|                lon|onground|  spi|squawk|      time|          velocity|vertrate|
+-----+-----------------+--------+-----------------+------------------+------+----------------+----------------+------------------+-------------------+--------+-----+------+----------+------------------+--------+
|false|5791.200000000001|AM217   |5897.880000000001|27.370562665503765|7c01c2|1.590364809802E9|1.590364798998E9|-34.50590515136719| 149.90424331353637|   false|false|  4067|1590364810|130.92069174119513|     0.0|
|false|          3398.52|SKW3355 |           3505.2|203.00887008282288|a1311e|1.590364809627E9|1.590364809498E9|35.968231201171875|-115.36251068115234|   false|false|  6125|1590364810|163.20122731231848| 8.45312|
+-----+-----------------+--------+-----------------+------------------+------+----------------+----------------+------------------+-------------------+--------+-----+------+----------+------------------+--------+
only showing top 2 rows

>>> df.write.format("org.apache.spark.sql.redis").option("table", "state_vectors").save()
22/12/01 22:12:45 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.

>>> df.printSchema()
root
 |-- alert: boolean (nullable = true)
 |-- baroaltitude: double (nullable = true)
 |-- callsign: string (nullable = true)
 |-- geoaltitude: double (nullable = true)
 |-- heading: double (nullable = true)
 |-- icao24: string (nullable = true)
 |-- lastcontact: double (nullable = true)
 |-- lastposupdate: double (nullable = true)
 |-- lat: double (nullable = true)
 |-- lon: double (nullable = true)
 |-- onground: boolean (nullable = true)
 |-- spi: boolean (nullable = true)
 |-- squawk: string (nullable = true)
 |-- time: integer (nullable = true)
 |-- velocity: double (nullable = true)
 |-- vertrate: double (nullable = true)

>>> df.distinct().count()
723098

```

Initial load of 12 hours of a day

```
# Keyspace
db0:keys=10222913,expires=0,avg_ttl=0
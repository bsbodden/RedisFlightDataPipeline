import redis
from redis.commands.search.field import (
    GeoField,
    NumericField,
    TagField,
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

r = redis.StrictRedis(decode_responses=True)
index_name = "state_vectors_idx"

# 127.0.0.1:6379> HGETALL "state_vectors:e835fe8ad408492f8891b202c5bad928"
#  1) "icao24"
#  2) "aa6fca"
#  3) "lon"
#  4) "-96.41122371592417"
#  5) "time"
#  6) "1590972620"
#  7) "lat"
#  8) "36.13151938228284"
#  9) "spi"
# 10) "false"
# 11) "squawk"
# 12) "2513"
# 13) "geoaltitude"
# 14) "12778.74"
# 15) "velocity"
# 16) "224.01954111173873"
# 17) "lastposupdate"
# 18) "1.590972616086E9"
# 19) "lastcontact"
# 20) "1.590972619876E9"
# 21) "vertrate"
# 22) "0.0"
# 23) "alert"
# 24) "false"
# 25) "heading"
# 26) "267.3675651310135"
# 27) "baroaltitude"
# 28) "12192.0"
# 29) "callsign"
# 30) "SWA170  "
# 31) "onground"
# 32) "false"


# Data Cleaning
# Delete all icao24 that have all NULL latitudes

deleted = 0
processed = 0
keys = r.keys("state_vectors:*")
for key in keys:
    flight = r.hgetall(key)
    if flight.keys() >= {
        "icao24",
        "lon",
        "lat",
        "time",
        "geoaltitude",
        "velocity",
        "callsign",
    }:
        if (
            flight["lon"] == "NaN"
            or flight["lat"] == "NaN"
            or flight["time"] == "NaN"
            or flight["geoaltitude"] == "NaN"
            or flight["velocity"] == "NaN"
            or flight["callsign"] == "NaN"
        ):
            deleted = deleted + 1
            r.delete(key)
        else:
            if "geo" not in flight:
                lon_lat = f"{flight['lon']},{flight['lat']}"
                r.hsetnx(key, "geo", lon_lat)
                processed = processed + 1
    else:
        deleted = deleted + 1
        r.delete(key)

print(f"Processed {processed} flights, deleted {deleted} bad records...")

schema = (
    TagField("icao24"),
    NumericField("time"),
    NumericField("geoaltitude"),
    NumericField("velocity"),
    TagField("callsign"),
    GeoField("geo"),
)

print("Creating index...")
r.ft(index_name).create_index(
    schema,
    definition=IndexDefinition(prefix=["state_vectors:"], index_type=IndexType.HASH),
)

print(r.ft(index_name).info())

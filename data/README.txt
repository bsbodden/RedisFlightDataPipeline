OpenSky Data Sample
===================

The data is provided by the OpenSky Network.

The OpenSky Network is a community-based receiver network which continuously collects air traffic surveillance data. Unlike other networks, OpenSky keeps the collected data forever and makes it available to researchers. With more than 2 trillion ADS-B and Mode S messages collected so far, the OpenSky Network exhibits the largest air traffic surveillance dataset of its kind.

You need to agree to the Terms and Conditions as stated in LICENSE.txt. Please read carefully.


State Vector Samples
=====================

This directory contains state vector sample data sets. State Vectors are our abstraction for tracking information.
The data sets cover a full day and are extracted every Tuesday night for the preceding day. We offer three different formats:
  - CSV
  - Avro
  - JSON
There is a file in either format for every hour. Be aware that CSV and JSON in particular are much larger after decompression.


Column Definitions
------------------

The following paragraphs are copied from our Impala Guide [4] and should give you an idea of the data.

time

This column contains the unix (aka POSIX or epoch) timestamp for which the state vector was valid. You'll find one state vector per second for each aircraft which was active within the coverage of OpenSky at that particular second. For more information on how these state vectors are generated, please refer to the API documentation [1]. In the above example, the time is 1480760792 which means that we are looking at a state vector that was valid on Saturday, 03-Dec-16 10:26:32 UTC. Tip: There are online tools [2] available for converting unix timestamps, just use Google.

icao24

This column contains the 24-bit ICAO transponder ID which can be used to track specific airframes over different flights. This ID should never change during a registration period of an airframe, which doesn't change very often. So if you are looking for a particular aircraft, try to find out its 24-bit transponder ID and filter by this column. In our data, it's represented as a 6 digit hexadecimal number (string). In our case, we are looking at the state of an aircraft using the transponder ID a0d724. If you look it up on databases like airframes.org, you'll find out that this transponder ID is used by an Airbus A306 owned by UPS. You will find this column in all tables.

lat/lon

These column contain the last known latitude and longitude of the aircraft. Coordinates are stored as decimal WGS84 coordinates. So here is what we know so far: On Saturday, 03-Dec-16 at 10:26:32 UTC, the UPS aircraft with transponder ID a0d724 was at position 37.89463883739407,-88.93331113068955. If you look it up on Google maps, it's somewhere in Illinois in the US.

velocity

This column contains the speed over ground of the aircraft in meters per second. In our example, the UPS aircraft flew over Illinois at a speed of 190.8504039695975 meters per second.

heading

This column represents the direction of movement (track angle) as the clockwise angle from the geographic north. Just a little side note for the aviation experts: you might want to complain that the term "heading" is not perfectly correct (if you are one of them you know what I mean) and something like "track" or "track angle" might be more correct. You are absolutely right and this might change in future releases, but for legacy reasons it's called "heading" for now. In our case, the aircraft flew into the direction 265.8263544365708° clockwise from geographic north, or in other words, to the west.

vertrate

This column contains the vertical speed of the aircraft in meters per second. A negative number indicates that the aircraft was descending, a positive number indicates a ascend respectively. In the above example, the UPS aircraft was neither ascending nor descending.

callsign

This column contains the callsign that was broadcast by the aircraft. Most airlines indicate the airline and the flight number in the callsign, but there is no unified system. In our example, the callsign indicates that this state vector belongs to UPS flight 858. By looking up the flightnumber on services like flightaware.com, you'll find out that this flight goes from Lousville to Phoenix every day.

onground

This flag indicates whether the aircraft is broadcasting surface positions (true) or airborne positions (false). Our UPS aircraft was airborne.

alert/spi

These two flags are special indicators used in ATC. If you need them, you'll know what they mean.

squawk

This 4-digit octal number is another transponder code which is used by ATC and pilots for identification purposes and indication of emergencies. Usually, ATC assigns squawks to aircraft when they enter their airspace via radio. In the above example, the UPS flight was assigned squawk "7775". See e.g. Wikipedia [3] for a list of special purpose squawks.

baroaltitude/geoaltitude

These two columns indicate the aircraft's altitudel. As the names suggest, baroaltitude is the altitude measured by the barometer and depends on factors such as weather, whereas geoaltitude is determined using the GNSS (GPS) sensor. In our case, the aircraft was flying at a geometric altitude (or height) of 9342.12 meters and a barometric altitude of 9144 meters. That makes a difference of almost 200 meters. You are likely to observe similar differences for aircraft in spatial and temporal vicinity. Note that due to its importance in aviation, barometric altitude will almost always be present, while the geometric altitude depends on the equipage of the aircraft.

lastposupdate

This unix timestamp indicates the age of the position. The position of the state vector above was already 87.64 seconds old at the time when the state vector was created (time) and should not be used any longer.

lastcontact

This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft. As long as the aircraft is flying in an airspace which is well-covered by OpenSky's receivers this timestamp should never be older than 1-2 seconds compared to the state vectors timestamp (time). Apparently, OpenSky's coverage in Illinois was not too good in December 2016 since the last contact indicates that the aircraft left the covered airspace already 82 seconds ago. OpenSky continues generating state vectors for 300 seconds after the last contact. Depending on your application, you can filter state vectors which are, e.g., older than 15 by adding a WHERE-clause to your query saying "WHERE time-lastcontact<=15". The relationship between the three timestamps explained so far is time > lastcontact >= lastposupdate.


Further Reading
---------------

[1] OpenSky API documentation
    https://opensky-network.org/apidoc/

[2] Time Stamp Converter Tools
    https://www.google.de/search?q=unix+timestamp+converter&cad=h

[3] Transponder Codes
    https://en.wikipedia.org/wiki/Transponder_(aeronautics)#Transponder_codes

[4] OpenSky Impala Guide
    https://opensky-network.org/impala-guide

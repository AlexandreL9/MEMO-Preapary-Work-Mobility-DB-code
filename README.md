# Real-Time Data Streams in MobilityDB
## MEMO-F-403 Preparatory work for the master thesis 
### Libert Alexandre - 2021

# Sources

Mahmoud SAKR and Esteban ZIMÁNYI, MobilityDB Workshop, July 31, 2021
Esteban Zimányi, BerlinMOD Benchmark on MobilityDB, August 8, 2021

## Parse data
### Create input table

    CREATE TABLE AISInput(
    T timestamp,
    TypeOfMobile varchar(50),
    MMSI integer,
    Latitude float,
    Longitude float,
    navigationalStatus varchar(50),
    ROT float,
    SOG float,
    COG float,
    Heading integer,
    IMO varchar(50),
    Callsign varchar(50),
    Name varchar(100),
    ShipType varchar(50),
    CargoType varchar(100),
    Width float,
    Length float,
    TypeOfPositionFixingDevice varchar(50),
    Draught float,
    Destination varchar(50),
    ETA varchar(50),
    DataSourceType varchar(50),
    SizeA float,
    SizeB float,
    SizeC float,
    SizeD float,
    Geom geometry(Point, 4326)
    );

 ### Parse csv
 
    SET datestyle = dmy;
    
    COPY AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus,
    ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length,
    TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType,
    SizeA, SizeB, SizeC, SizeD)
    FROM '/home/aisdk_20170623.csv' DELIMITER ',' CSV HEADER;
	
	UPDATE AISInput SET NavigationalStatus = CASE
	NavigationalStatus WHEN 'Unknown value' THEN NULL END, IMO =
	CASE IMO WHEN 'Unknown' THEN NULL END, ShipType = CASE
	ShipType WHEN 'Undefined' THEN NULL END,
	TypeOfPositionFixingDevice = CASE TypeOfPositionFixingDevice
	WHEN 'Undefined' THEN NULL END, Geom = ST_SetSRID(
	ST_MakePoint( Longitude, Latitude ), 4326);
	
    CREATE TABLE AISInputFiltered AS
    SELECT DISTINCT ON(MMSI,T) *
    FROM AISInput
    WHERE Longitude BETWEEN -16.1 and 32.88 AND Latitude BETWEEN 40.18 AND 84.17;

### Create trajectories table 
**Important :**  use 4326 as SRID 

As we need a time reference for the table partioning we will use the timestamp of the first emission of each mmsi

    CREATE TABLE firstTimestamp(
    mmsi integer,
    t timestamp without time zone);
    
    INSERT INTO firstTimestamp
    SELECT mmsi, min(T)
    from AISInputFiltered
    GROUP BY mmsi

#### Non partitioned
    CREATE TABLE Ships(MMSI, tt, Trip, SOG, COG) AS
    SELECT aisf.MMSI,
    (SELECT firsttimestamp.T from firsttimestamp where firsttimestamp.mmsi = aisf.mmsi),
    tgeompointseq(array_agg(tgeompointinst( ST_Transform(aisf.Geom, 4326), aisf.T) ORDER BY aisf.T)),
    tfloatseq(array_agg(tfloatinst(aisf.SOG, aisf.T) ORDER BY aisf.T) FILTER (WHERE aisf.SOG IS NOT NULL)),
    tfloatseq(array_agg(tfloatinst(aisf.COG, aisf.T) ORDER BY aisf.T) FILTER (WHERE aisf.COG IS NOT NULL))
    FROM AISInputFiltered as aisf
    GROUP BY aisf.MMSI;
    
    ALTER TABLE Ships ADD COLUMN Traj geometry;
    UPDATE Ships SET Traj= ST_SetSRID(trajectory(Trip), 4326);

    

    
#### Partititoned 

Approx 400 rows/partition

    CREATE TABLE Ships_P_
    (
    MMSI integer,
    t timestamp,
    Trip tgeompoint,
    SOG tfloat,
    COG tfloat,
    traj geometry
    )PARTITION BY RANGE(t);
    
    
    CREATE TABLE Ships_P_0_04 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 00:00:00') TO ('2017-06-23 00:00:04');
    CREATE TABLE Ships_P_04_10 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 00:00:04') TO ('2017-06-23 00:00:10');
    CREATE TABLE Ships_P_10_20 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 00:00:10') TO ('2017-06-23 00:00:20');
    CREATE TABLE Ships_P_20_1 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 00:00:20') TO ('2017-06-23 00:01:00');
       
    CREATE TABLE Ships_P_01_03 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 00:01:00') TO ('2017-06-23 00:03:00');
    CREATE TABLE Ships_P_03_20 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 00:03:00') TO ('2017-06-23 00:20:00');
    CREATE TABLE Ships_P_20_4 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 00:20:00') TO ('2017-06-23 04:00:00');
    CREATE TABLE Ships_P_4_8 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 04:00:00') TO ('2017-06-23 08:00:00');
    CREATE TABLE Ships_P_8_12 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 08:00:00') TO ('2017-06-23 12:00:00');
    CREATE TABLE Ships_P_12_24 PARTITION OF Ships_P_
       FOR VALUES FROM ('2017-06-23 12:00:00') TO ('2017-06-23 24:00:00');
       

    INSERT INTO Ships_P_
    SELECT *
    FROM Ships
       

## Benchmark

Use **Ships_P** for partitioned 
Use **Ships** for non partitioned

### Range query

Return the list of ships that visited a rectange around the city of Kolding

    SELECT mmsi from AISInputfiltered_1 where ST_Intersects(geom, ST_SetSRID(geometry 'Polygon((9 55.2, 9 55.8
    ,10 55.8,10 55.2,9 55.2))', 4326))

### Temporal query
Count the number of trips that were active during each hour after 12h in June 23, 2017

    WITH TimeSplit(Period) AS (
    SELECT period(H, H + interval '1 hour')
    FROM generate_series(timestamptz '2017-06-23 12:00:00',
    timestamptz '2017-06-23 23:00:00', interval '1 hour') AS H )
    SELECT Period, COUNT(*)
    FROM TimeSplit S, Ships T
    WHERE S.Period && T.Trip AND atPeriod(Trip, Period) IS NOT NULL
    GROUP BY S.Period
    ORDER BY S.Period;

### Distance query 

List the closest ships to the city of Kolding the 23 June 2017 at 8 AM

    SELECT T.mmsi, trip <-> tgeompointinst(ST_SetSRID( ST_Point(55.493752, 9.471496),4326), '2017-06-23 08:00:00') AS MinDistance
    FROM Ships T
    GROUP BY T.mmsi, MinDistance
    ORDER BY MinDistance ASC

### Nearest-Neighbor Query
First, add three cities in the table points:

| 1 | Kolding |
| 2 |  Copenhague |
| 3 | Aarhus |



    CREATE TABLE Points (
    PointId integer PRIMARY KEY,
    Geom Geometry(Point)
    );
    
    INSERT INTO Points VALUES (1,geometry(ST_SetSRID( ST_Point(55.493752, 9.471496),4326))),
    (2,geometry(ST_SetSRID( ST_Point(55.671823, 12.563272),4326))),
    (3,geometry(ST_SetSRID( ST_Point(56.156662, 10.206493),4326)))


For each trip from Trips, list the two cities from the three that have been closest to that ship.


    WITH TripsTraj AS (
    SELECT *, traj AS Trajectory FROM Ships )
    SELECT T.mmsi, P1.PointId, P1.Distance
    FROM TripsTraj T CROSS JOIN LATERAL (
    SELECT P.PointId, T.Trajectory <-> P.Geom AS Distance
    FROM Points P
    ORDER BY Distance LIMIT 2 ) AS P1
    ORDER BY T.mmsi, T.mmsi, P1.Distance;



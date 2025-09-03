# Flink Geo UDFs

## Description

This project provides geospatial functions for Apache Flink. It is a Flink-compatible port of the [KSQLGeo](https://github.com/wlaforest/KSQLGeo) project, which provides geospatial functions for ksqlDB. The functions in this library are underpinned by [Spatial4j](https://github.com/locationtech/spatial4j).

## Features

The following geospatial functions are available:

### Scalar Functions (UDFs)

1. **geo_area(geometry)** - Calculates the area of a shape in square degrees
2. **geo_contained(lat, lon, geometry)** - Tests if a point is contained within a geometry
3. **geo_contained(geometry1, geometry2)** - Tests if geometry1 is contained within geometry2
4. **geo_intersected(geometry1, geometry2)** - Tests if two geometries intersect
5. **geo_geohash(lat, lon [, precision])** - Calculates the geohash of a point

### Table Functions (UDTFs)

1. **geo_covering_geohashes(geometry [, precision])** - Emits all geohashes that cover a geometry

## Supported Formats

### Encodings

Flink Geo UDFs uses the deserialization from Spatial4J and currently supports GeoJSON and WKT. For more information about the specifics of this see https://github.com/locationtech/spatial4j/blob/master/FORMATS.md

## Building Flink Geo UDFs

### Prerequisites

- Java 8 or higher
- Maven 3.x

### Building the code

First obtain the code by cloning the Git repository:

```bash
git clone https://github.com/wlaforest/flink-geo-udfs.git
cd flink-geo-udfs
```

Then build the code using Maven:

```bash
mvn clean package
```

This will create an uber jar in the `target` directory.

## Installation and Usage

### Adding UDFs to Flink

1. If you are using Apache Flink copy the uber jar to your Flink lib directory:

```bash
cp target/flink-geo-udfs-1.0.0.jar $FLINK_HOME/lib/
```

If you are using fully managed Flink in Confluent Cloud, you can go to the environment and upload the jar as an artifact. For more information, see [Create a User-Defined Function with Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/how-to-guides/create-udf.html?ajs_aid=05c4f5b9-b963-4c91-8574-b7ed1ba3dd26&ajs_uid=165).

2. Restart your Flink cluster to pick up the new functions.

### Registering Functions in Flink SQL

You need to register each function before using it in Flink SQL:

**Confluent Cloud Flink:**
```sql
-- Register scalar functions
CREATE FUNCTION geo_area AS 'com.github.wlaforest.flink.geo.udfs.GeoAreaUDF' USING JAR 'confluent-artifact://cfa-xxxxxx';
CREATE FUNCTION geo_contained AS 'com.github.wlaforest.flink.geo.udfs.GeoContainedUDF' USING JAR 'confluent-artifact://cfa-xxxxxx';
CREATE FUNCTION geo_intersected AS 'com.github.wlaforest.flink.geo.udfs.GeoIntersectedUDF' USING JAR 'confluent-artifact://cfa-xxxxxx';
CREATE FUNCTION geo_geohash AS 'com.github.wlaforest.flink.geo.udfs.GeoHashUDF' USING JAR 'confluent-artifact://cfa-xxxxxx';

-- Register table function
CREATE FUNCTION geo_covering_geohashes AS 'com.github.wlaforest.flink.geo.udfs.GeoCoveringGeoHashesUDTF' LANGUAGE JAVA;
```

<details>
<summary><strong>Click here for Apache Flink syntax</strong></summary>

```sql
-- Register scalar functions
CREATE FUNCTION geo_area AS 'com.github.wlaforest.flink.geo.udfs.GeoAreaUDF';
CREATE FUNCTION geo_contained AS 'com.github.wlaforest.flink.geo.udfs.GeoContainedUDF';
CREATE FUNCTION geo_intersected AS 'com.github.wlaforest.flink.geo.udfs.GeoIntersectedUDF';
CREATE FUNCTION geo_geohash AS 'com.github.wlaforest.flink.geo.udfs.GeoHashUDF';

-- Register table function
CREATE FUNCTION geo_covering_geohashes AS 'com.github.wlaforest.flink.geo.udfs.GeoCoveringGeoHashesUDTF';
```

</details>

### Using in Flink Table API (Java/Scala)

```java
// Example in Java
TableEnvironment tableEnv = TableEnvironment.create(settings);

// Register the functions
tableEnv.createTemporarySystemFunction("geo_area", GeoAreaUDF.class);
tableEnv.createTemporarySystemFunction("geo_contained", GeoContainedUDF.class);
tableEnv.createTemporarySystemFunction("geo_intersected", GeoIntersectedUDF.class);
tableEnv.createTemporarySystemFunction("geo_geohash", GeoHashUDF.class);
tableEnv.createTemporarySystemFunction("geo_covering_geohashes", GeoCoveringGeoHashesUDTF.class);
```

## Examples

### Creating test data

**Confluent Cloud Flink:**
```sql
CREATE TABLE schools (
    name STRING,
    unity BOOLEAN,
    wkt STRING
);

INSERT INTO schools VALUES 
    ('Madison', true, 
     'POLYGON((-77.27483600429103 38.89521905950339,-77.29131549647853 38.892012508280466,-77.31277316859767 38.89254694353762,-77.32066959193752 38.901097360742895,-77.31277316859767 38.90750949802689,-77.29938358119533 38.90697517537252,-77.30384677699611 38.91378748795597,-77.29818195155666 38.916325241169524,-77.30556339076564 38.92927972487108,-77.29869693568752 38.929413263931195,-77.29200214198634 38.93315225554382,-77.28307575038478 38.92741015163275,-77.2705444698672 38.92126692120997,-77.26608127406642 38.916191677473286,-77.2511467342715 38.91819510652208,-77.24634021571681 38.91191750646839,-77.27483600429103 38.89521905950339))'),
    ('Oakton', true,
     'POLYGON((-77.3029731301166 38.871966195174494,-77.26846919334902 38.89027347822612,-77.29662165916933 38.88492877364056,-77.34846339500918 38.89575138309681,-77.33249888695254 38.8699614616396,-77.3029731301166 38.871966195174494))');
```

<details>
<summary><strong>Click here for Apache Flink syntax</strong></summary>

```sql
CREATE TABLE schools (
    name STRING,
    unity BOOLEAN,
    wkt STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'schools',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

INSERT INTO schools VALUES 
    ('Madison', true, 
     'POLYGON((-77.27483600429103 38.89521905950339,-77.29131549647853 38.892012508280466,-77.31277316859767 38.89254694353762,-77.32066959193752 38.901097360742895,-77.31277316859767 38.90750949802689,-77.29938358119533 38.90697517537252,-77.30384677699611 38.91378748795597,-77.29818195155666 38.916325241169524,-77.30556339076564 38.92927972487108,-77.29869693568752 38.929413263931195,-77.29200214198634 38.93315225554382,-77.28307575038478 38.92741015163275,-77.2705444698672 38.92126692120997,-77.26608127406642 38.916191677473286,-77.2511467342715 38.91819510652208,-77.24634021571681 38.91191750646839,-77.27483600429103 38.89521905950339))'),
    ('Oakton', true,
     'POLYGON((-77.3029731301166 38.871966195174494,-77.26846919334902 38.89027347822612,-77.29662165916933 38.88492877364056,-77.34846339500918 38.89575138309681,-77.33249888695254 38.8699614616396,-77.3029731301166 38.871966195174494))');
```

</details>

### Using scalar functions

```sql
-- Calculate area
SELECT name, geo_area(wkt) as area_sq_degrees
FROM schools;

-- Check containment with a point
SELECT * FROM schools
WHERE geo_contained(38.900495, -77.255953, wkt);

-- Check intersection between geometries
SELECT * FROM schools
WHERE geo_intersected(wkt, 
    'POLYGON((-77.25595325282619 38.90049514948347,-77.25389331630275 38.90503717840573,-77.289598882709 38.91171610485367,-77.2760376339297 38.889539649049034,-77.25595325282619 38.90049514948347))');

-- Generate geohash for a point
SELECT geo_geohash(38.900495, -77.255953, 7) as geohash;
```

### Using table function for re-keying

```sql
-- Generate multiple records with different geohash keys
SELECT schools.*, geohash
FROM schools,
LATERAL TABLE(geo_covering_geohashes(schools.wkt, 7)) AS T(geohash);
```


## Migration from KSQLGeo

### Key Differences

1. **Function Registration**: In Flink, you need to explicitly register functions before use, unlike ksqlDB which auto-discovers them.

2. **Configuration**: Flink UDFs don't support the same configuration mechanism as ksqlDB. The Spatial4J configuration parameters (geo mode, normWrapLongitude) are currently set to defaults.

3. **Error Handling**: Flink UDFs throw RuntimeExceptions for parsing errors rather than checked exceptions.

4. **Table Function Syntax**: The UDTF syntax in Flink uses `LATERAL TABLE()` instead of ksqlDB's syntax.

### Migration Steps

1. Replace ksqlDB function calls with Flink equivalents (function names remain the same)
2. Register all functions in your Flink environment
3. Update any UDTF usage to use Flink's `LATERAL TABLE()` syntax
4. Handle exceptions appropriately (Flink uses RuntimeExceptions)

## Performance Considerations

- The UDFs create a new `Spatial4JHelper` instance per task, which is efficient for parallel processing
- Geohash precision affects performance - higher precision means more geohashes generated for covering operations
- WKT and GeoJSON is parsed with each function invocation.  Once Flink has native geo types this will be much better

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GNU Lesser General Public License v3.0 (LGPL-3.0).

The LGPL is a permissive copyleft license that allows this library to be linked with proprietary software, while ensuring that modifications to the library itself remain open source. See the [LICENSE](LICENSE) file for the full license text.



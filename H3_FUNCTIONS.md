# H3 Functions for Apache Flink

This document describes the H3 functions available in the Flink Geo UDFs library. H3 is Uber's hierarchical hexagonal geospatial indexing system.

## Available H3 Functions

### 1. geo_h3 (UDF)

Generates an H3 cell ID for a given latitude/longitude point.

**Syntax:**
```sql
geo_h3(lat, lon)           -- Uses default resolution 7
geo_h3(lat, lon, resolution)  -- Uses specified resolution (0-15)
```

**Parameters:**
- `lat`: Double - Latitude (-90 to 90)
- `lon`: Double - Longitude (-180 to 180)
- `resolution`: Integer (optional) - H3 resolution level (0-15, default is 7)

**Returns:**
- String - H3 cell ID as a 15-character hex string

**Examples:**
```sql
-- Register the function
CREATE FUNCTION geo_h3 AS 'com.github.wlaforest.flink.geo.udfs.GeoH3UDF';

-- Generate H3 cell at default resolution (7) for San Francisco
SELECT geo_h3(37.7749, -122.4194);
-- Returns: 87283472bffffff

-- Generate H3 cell at resolution 10 for New York City
SELECT geo_h3(40.7128, -74.0060, 10);
-- Returns: 8a2a1072b427fff

-- Use with streaming data
SELECT user_id, geo_h3(lat, lon, 8) as h3_cell
FROM user_locations;
```

### 2. geo_covering_h3 (UDTF)

Returns all H3 cells that cover a given geometry. This is useful for spatial indexing and partitioning.

**Syntax:**
```sql
LATERAL TABLE(geo_covering_h3(geometry))           -- Uses default resolution 7
LATERAL TABLE(geo_covering_h3(geometry, resolution))  -- Uses specified resolution
```

**Parameters:**
- `geometry`: String - WKT or GeoJSON encoded geometry
- `resolution`: Integer (optional) - H3 resolution level (0-15, default is 7)

**Returns:**
- Table with single column containing H3 cell IDs as strings

**Examples:**
```sql
-- Register the function
CREATE FUNCTION geo_covering_h3 AS 'com.github.wlaforest.flink.geo.udfs.GeoCoveringH3UDTF';

-- Get all H3 cells covering a polygon
SELECT region_id, h3_cell
FROM regions R,
LATERAL TABLE(geo_covering_h3(R.wkt_geometry)) AS L(h3_cell);

-- Get H3 cells at resolution 6 for spatial partitioning
SELECT order_id, h3_cell
FROM delivery_zones D,
LATERAL TABLE(geo_covering_h3(D.boundary, 6)) AS L(h3_cell);

-- Use with GeoJSON geometries
SELECT area_name, h3_cell
FROM areas A,
LATERAL TABLE(geo_covering_h3(A.geojson_boundary, 8)) AS L(h3_cell);
```

## H3 Resolution Levels

H3 uses resolution levels from 0 (coarsest) to 15 (finest):

| Resolution | Average Hexagon Area |
|-----------|---------------------|
| 0         | 4,250,546.8 km²     |
| 1         | 607,220.98 km²      |
| 2         | 86,745.85 km²       |
| 3         | 12,392.26 km²       |
| 4         | 1,770.32 km²        |
| 5         | 252.90 km²          |
| 6         | 36.13 km²           |
| 7         | 5.16 km²            |
| 8         | 0.737 km²           |
| 9         | 0.105 km²           |
| 10        | 0.015 km²           |
| 11        | 0.002 km²           |
| 12        | 0.0003 km²          |
| 13        | 0.00004 km²         |
| 14        | 0.000006 km²        |
| 15        | 0.0000009 km²       |

## Use Cases

### Spatial Indexing and Partitioning
```sql
-- Partition data by H3 cells for efficient spatial queries
CREATE TABLE user_events_partitioned AS
SELECT *, geo_h3(lat, lon, 7) as h3_cell
FROM user_events;

-- Index by H3 for fast spatial lookups
CREATE INDEX idx_h3 ON user_events_partitioned(h3_cell);
```

### Geofencing and Proximity Analysis
```sql
-- Find events within the same H3 cell (approximate proximity)
SELECT a.event_id as event1, b.event_id as event2
FROM user_events_partitioned a
JOIN user_events_partitioned b ON a.h3_cell = b.h3_cell
WHERE a.event_id < b.event_id;
```

### Spatial Aggregation
```sql
-- Count events per H3 cell
SELECT h3_cell, COUNT(*) as event_count
FROM user_events_partitioned
GROUP BY h3_cell;
```

### Coverage Analysis
```sql
-- Find all H3 cells that intersect with delivery zones
SELECT delivery_zone, COUNT(DISTINCT h3_cell) as coverage_cells
FROM delivery_zones D,
LATERAL TABLE(geo_covering_h3(D.boundary, 8)) AS L(h3_cell)
GROUP BY delivery_zone;
```

## Performance Considerations

1. **Resolution Choice**: Higher resolution = more precise but more cells
2. **Cell Limits**: Large geometries at high resolution may hit H3's internal limits
3. **Memory Usage**: The `geo_covering_h3` function may return many cells for large areas
4. **Indexing**: H3 cells work well as partition keys or index columns

## Integration with Existing Geohash Functions

The H3 functions complement the existing geohash functions in this library:
- Use geohashes for simple rectangular grids
- Use H3 for hexagonal grids with better area uniformity
- Both support hierarchical spatial indexing

## Error Handling

- Invalid coordinates (lat/lon out of range) throw `IllegalArgumentException`
- Invalid resolution levels throw `IllegalArgumentException`
- Invalid geometry strings throw `GeometryParseException`
- Null inputs return null results safely

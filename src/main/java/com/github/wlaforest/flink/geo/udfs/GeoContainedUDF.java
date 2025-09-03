package com.github.wlaforest.flink.geo.udfs;

import com.github.wlaforest.geo.GeometryParseException;
import com.github.wlaforest.geo.Spatial4JHelper;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink UDF function to test containment of a point in a geometry.
 * Geometry can be encoded in WKT or GeoJSON.
 * null parameters will always result in false
 * 
 * Usage in Flink SQL:
 * CREATE FUNCTION geo_contained AS 'com.github.wlaforest.flink.geo.udfs.GeoContainedUDF';
 * 
 * Examples:
 * SELECT geo_contained(lat, lon, wkt_polygon) FROM table;
 * SELECT geo_contained('40.7128', '-74.0060', wkt_polygon) FROM table;
 * SELECT geo_contained(wkt_geom1, wkt_geom2) FROM table;
 */
public class GeoContainedUDF extends ScalarFunction {
    
    private transient Spatial4JHelper spatial4JHelper;
    
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        spatial4JHelper = new Spatial4JHelper();
        // In Flink, we could read configuration from the context if needed
        // For now, using default configuration
        spatial4JHelper.configure(null);
    }
    
    /**
     * Determines if a double value lat/long is inside or outside the geometry
     * 
     * @param latitude the latitude of the point
     * @param longitude the longitude of the point
     * @param geo WKT or GeoJSON Encoded Geometry to check for enclosure
     * @return true if the point is contained within the geometry
     */
    public Boolean eval(Double latitude, Double longitude, String geo) {
        try {
            if (geo == null || latitude == null || longitude == null) {
                return false;
            }
            return spatial4JHelper.contained(geo, latitude, longitude);
        } catch (GeometryParseException e) {
            throw new RuntimeException("Failed to parse geometry: " + e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in geo_contained: " + e.getMessage(), e);
        }
    }
    
    /**
     * Determines if a String value lat/long is inside or outside the geometry
     * 
     * @param latitude the latitude of the point as a string
     * @param longitude the longitude of the point as a string
     * @param geo WKT or GeoJSON Encoded Geometry to check for enclosure
     * @return true if the point is contained within the geometry
     */
    public Boolean eval(String latitude, String longitude, String geo) {
        if (latitude == null || longitude == null) {
            return false;
        }
        try {
            return eval(Double.parseDouble(latitude), Double.parseDouble(longitude), geo);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid latitude or longitude format: " + e.getMessage(), e);
        }
    }
    
    /**
     * Determines if geo1 is contained within geo2
     * 
     * @param geo1 WKT or GeoJSON Encoded Geometry for the enclosure
     * @param geo2 WKT or GeoJSON Encoded Geometry to check for enclosure in geo1
     * @return true if geo1 is contained within geo2
     */
    public Boolean eval(String geo1, String geo2) {
        try {
            if (geo1 == null || geo2 == null) {
                return false;
            }
            return spatial4JHelper.contained(geo1, geo2);
        } catch (GeometryParseException e) {
            throw new RuntimeException("Failed to parse geometry: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean isDeterministic() {
        return true;
    }
}

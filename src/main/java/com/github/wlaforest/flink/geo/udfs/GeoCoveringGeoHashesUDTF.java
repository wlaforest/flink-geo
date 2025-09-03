package com.github.wlaforest.flink.geo.udfs;

import com.github.wlaforest.geo.GeometryParseException;
import com.github.wlaforest.geo.Spatial4JHelper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Flink UDTF for computing the geohashes to completely cover a geometry.
 * It will calculate all the geohashes a geometry falls in.
 * This is very useful for partitioning for the distributed join
 * 
 * Usage in Flink SQL:
 * CREATE FUNCTION geo_covering_geohashes AS 'com.github.wlaforest.flink.geo.udfs.GeoCoveringGeoHashesUDTF';
 * 
 * Example:
 * SELECT T.*, geohash 
 * FROM myTable T, 
 * LATERAL TABLE(geo_covering_geohashes(T.wkt_column)) AS L(geohash);
 * 
 * Or with precision:
 * SELECT T.*, geohash 
 * FROM myTable T, 
 * LATERAL TABLE(geo_covering_geohashes(T.wkt_column, 7)) AS L(geohash);
 */
@FunctionHint(output = @DataTypeHint("STRING"))
public class GeoCoveringGeoHashesUDTF extends TableFunction<String> {
    
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
     * Takes WKT or GeoJSON Encoded Geometry and a geohash granularity and computes all geohash
     * bins the geometry falls in. This is helpful for re-keying a stream
     * 
     * @param geo WKT or GeoJSON encoded geometry
     * @param precision what level of precision? Goes from 1-12
     */
    public void eval(String geo, Integer precision) {
        if (geo == null || precision == null) {
            return;
        }
        
        if (precision < 1 || precision > 12) {
            throw new IllegalArgumentException("Precision must be between 1 and 12");
        }
        
        try {
            List<String> geohashes = spatial4JHelper.coveringGeoHashes(geo, precision);
            for (String geohash : geohashes) {
                collect(geohash);
            }
        } catch (GeometryParseException e) {
            throw new RuntimeException("Failed to parse geometry: " + e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in geo_covering_geohashes: " + e.getMessage(), e);
        }
    }
    
    /**
     * Takes WKT or GeoJSON Encoded Geometry and computes all geohash bins
     * the geometry falls in using default precision of 7
     * 
     * @param geo WKT or GeoJSON encoded geometry
     */
    public void eval(String geo) {
        eval(geo, 7);
    }
}

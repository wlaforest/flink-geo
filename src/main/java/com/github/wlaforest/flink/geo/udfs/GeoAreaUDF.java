package com.github.wlaforest.flink.geo.udfs;

import com.github.wlaforest.geo.GeometryParseException;
import com.github.wlaforest.geo.Spatial4JHelper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink UDF to calculate the area of a shape
 * 
 * Usage in Flink SQL:
 * CREATE FUNCTION geo_area AS 'com.github.wlaforest.flink.geo.udfs.GeoAreaUDF';
 * SELECT geo_area(wkt_column) FROM table;
 */
@FunctionHint(
    output = @DataTypeHint("DOUBLE"),
    input = @DataTypeHint("STRING")
)
public class GeoAreaUDF extends ScalarFunction {
    
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
     * Takes WKT or GeoJSON Encoded Geometry and calculates the area in square degrees
     * 
     * @param geo WKT or GeoJSON Encoded Geometry to calculate area for
     * @return area in square degrees
     * @throws GeometryParseException if the geometry cannot be parsed
     */
    public Double eval(String geo) {
        if (geo == null || geo.trim().isEmpty()) {
            return null;
        }
        
        try {
            return spatial4JHelper.area(geo);
        } catch (GeometryParseException e) {
            throw new RuntimeException("Failed to parse geometry: " + e.getMessage(), e);
        }
    }
    
    // Optional: Override isDeterministic() if the function is deterministic
    @Override
    public boolean isDeterministic() {
        return true;
    }
}

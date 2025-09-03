package com.github.wlaforest.flink.geo.udfs;

import org.apache.flink.table.functions.FunctionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GeoAreaUDFTest {
    
    private GeoAreaUDF geoAreaUDF;
    
    @BeforeEach
    public void setUp() throws Exception {
        geoAreaUDF = new GeoAreaUDF();
        // For testing, we can open without a FunctionContext
        geoAreaUDF.open(null);
    }
    
    @Test
    public void testAreaCalculation() {
        // Test with a simple square polygon
        String wkt = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))";
        Double area = geoAreaUDF.eval(wkt);
        
        assertNotNull(area);
        assertTrue(area > 0);
        // Approximate area of 1 degree square
        assertTrue(area > 0.9 && area < 1.1);
    }
    
    @Test
    public void testAreaWithGeoJSON() {
        // Test with GeoJSON format
        String geoJson = "{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}";
        Double area = geoAreaUDF.eval(geoJson);
        
        assertNotNull(area);
        assertTrue(area > 0);
    }
    
    @Test
    public void testNullInput() {
        Double area = geoAreaUDF.eval(null);
        assertNull(area);
    }
    
    @Test
    public void testEmptyInput() {
        Double area = geoAreaUDF.eval("");
        assertNull(area);
    }
    
    @Test
    public void testInvalidGeometry() {
        assertThrows(RuntimeException.class, () -> {
            geoAreaUDF.eval("INVALID GEOMETRY");
        });
    }
    
    @Test
    public void testDeterministic() {
        assertTrue(geoAreaUDF.isDeterministic());
    }
}

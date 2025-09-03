/*
 * Flink Geo UDFs - Geospatial User-Defined Functions for Apache Flink
 * Copyright (C) 2024 Will LaForest
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

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

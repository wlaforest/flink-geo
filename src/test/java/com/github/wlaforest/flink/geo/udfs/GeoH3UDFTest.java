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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GeoH3UDFTest {
    
    private GeoH3UDF geoH3UDF;
    
    @BeforeEach
    public void setUp() throws Exception {
        geoH3UDF = new GeoH3UDF();
        // For testing, we can open without a FunctionContext
        geoH3UDF.open(null);
    }
    
    @Test
    public void testH3WithDefaultResolution() {
        // Test with San Francisco coordinates
        Double lat = 37.7749;
        Double lon = -122.4194;
        String h3Cell = geoH3UDF.eval(lat, lon);
        
        assertNotNull(h3Cell);
        assertFalse(h3Cell.isEmpty());
        // H3 addresses are 15 character hex strings
        assertEquals(15, h3Cell.length());
        // Should start with '8' for resolution 7
        assertTrue(h3Cell.startsWith("87"));
    }
    
    @Test
    public void testH3WithSpecifiedResolution() {
        Double lat = 37.7749;
        Double lon = -122.4194;
        
        // Test different resolutions
        String h3Res0 = geoH3UDF.eval(lat, lon, 0);
        String h3Res5 = geoH3UDF.eval(lat, lon, 5);
        String h3Res10 = geoH3UDF.eval(lat, lon, 10);
        String h3Res15 = geoH3UDF.eval(lat, lon, 15);
        
        assertNotNull(h3Res0);
        assertNotNull(h3Res5);
        assertNotNull(h3Res10);
        assertNotNull(h3Res15);
        
        // Different resolutions should produce different results
        assertNotEquals(h3Res0, h3Res5);
        assertNotEquals(h3Res5, h3Res10);
        assertNotEquals(h3Res10, h3Res15);
        
        // All should be 15 characters
        assertEquals(15, h3Res0.length());
        assertEquals(15, h3Res5.length());
        assertEquals(15, h3Res10.length());
        assertEquals(15, h3Res15.length());
    }
    
    @Test
    public void testH3Consistency() {
        Double lat = 40.7128;
        Double lon = -74.0060;
        
        // Multiple calls should return the same result
        String h3Cell1 = geoH3UDF.eval(lat, lon, 8);
        String h3Cell2 = geoH3UDF.eval(lat, lon, 8);
        
        assertEquals(h3Cell1, h3Cell2);
    }
    
    @Test
    public void testNullInputs() {
        // Test null latitude
        String result1 = geoH3UDF.eval(null, -122.4194);
        assertNull(result1);
        
        // Test null longitude
        String result2 = geoH3UDF.eval(37.7749, null);
        assertNull(result2);
        
        // Test both null
        String result3 = geoH3UDF.eval(null, null);
        assertNull(result3);
        
        // Test null resolution
        String result4 = geoH3UDF.eval(37.7749, -122.4194, null);
        assertNull(result4);
    }
    
    @Test
    public void testInvalidLatitude() {
        // Test latitude out of bounds
        assertThrows(IllegalArgumentException.class, () -> {
            geoH3UDF.eval(91.0, -122.4194);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            geoH3UDF.eval(-91.0, -122.4194);
        });
    }
    
    @Test
    public void testInvalidLongitude() {
        // Test longitude out of bounds
        assertThrows(IllegalArgumentException.class, () -> {
            geoH3UDF.eval(37.7749, 181.0);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            geoH3UDF.eval(37.7749, -181.0);
        });
    }
    
    @Test
    public void testInvalidResolution() {
        Double lat = 37.7749;
        Double lon = -122.4194;
        
        // Test resolution out of bounds
        assertThrows(IllegalArgumentException.class, () -> {
            geoH3UDF.eval(lat, lon, -1);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            geoH3UDF.eval(lat, lon, 16);
        });
    }
    
    @Test
    public void testValidBoundaryCoordinates() {
        // Test boundary coordinates
        String northPole = geoH3UDF.eval(90.0, 0.0, 7);
        String southPole = geoH3UDF.eval(-90.0, 0.0, 7);
        String eastBound = geoH3UDF.eval(0.0, 180.0, 7);
        String westBound = geoH3UDF.eval(0.0, -180.0, 7);
        
        assertNotNull(northPole);
        assertNotNull(southPole);
        assertNotNull(eastBound);
        assertNotNull(westBound);
    }
    
    @Test
    public void testValidResolutionBounds() {
        Double lat = 0.0;
        Double lon = 0.0;
        
        // Test minimum resolution
        String h3Min = geoH3UDF.eval(lat, lon, 0);
        assertNotNull(h3Min);
        
        // Test maximum resolution
        String h3Max = geoH3UDF.eval(lat, lon, 15);
        assertNotNull(h3Max);
    }
    
    @Test
    public void testDeterministic() {
        assertTrue(geoH3UDF.isDeterministic());
    }
}

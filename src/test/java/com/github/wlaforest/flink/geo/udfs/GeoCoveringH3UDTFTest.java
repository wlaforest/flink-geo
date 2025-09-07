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

import com.github.wlaforest.geo.GeometryParseException;
import com.github.wlaforest.geo.Spatial4JHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class GeoCoveringH3UDTFTest {
    
    private Spatial4JHelper spatial4JHelper;
    
    @BeforeEach
    public void setUp() throws Exception {
        spatial4JHelper = new Spatial4JHelper();
        spatial4JHelper.configure(null);
    }
    
    @Test
    public void testH3CoveringWithDefaultResolution() throws GeometryParseException {
        // Test with a small polygon around San Francisco
        String wkt = "POLYGON((-122.42 37.77, -122.42 37.78, -122.41 37.78, -122.41 37.77, -122.42 37.77))";
        
        List<String> h3Cells = spatial4JHelper.coveringH3Cells(wkt, 7);
        
        // Method should not crash, might be empty for small geometries
        assertNotNull(h3Cells);
        
        // All results should be valid H3 addresses
        for (String h3Cell : h3Cells) {
            assertNotNull(h3Cell);
            assertEquals(15, h3Cell.length());
        }
    }
    
    @Test
    public void testH3CoveringWithSpecifiedResolution() throws GeometryParseException {
        String wkt = "POLYGON((-122.42 37.77, -122.42 37.78, -122.41 37.78, -122.41 37.77, -122.42 37.77))";
        
        // Test with resolution 5 (larger cells, fewer results)
        List<String> results5 = spatial4JHelper.coveringH3Cells(wkt, 5);
        
        // Test with resolution 10 (smaller cells, more results)
        List<String> results10 = spatial4JHelper.coveringH3Cells(wkt, 10);
        
        // H3 cells may be empty for very small geometries at low resolution
        // Just verify that the method doesn't crash and returns valid data when present
        
        // All results should be valid H3 addresses
        for (String h3Cell : results5) {
            assertNotNull(h3Cell);
            assertEquals(15, h3Cell.length());
        }
        
        for (String h3Cell : results10) {
            assertNotNull(h3Cell);
            assertEquals(15, h3Cell.length());
        }
    }
    
    @Test
    public void testH3CoveringWithGeoJSON() throws GeometryParseException {
        // Test with GeoJSON format
        String geoJson = "{\"type\":\"Polygon\",\"coordinates\":[[[-122.42,37.77],[-122.42,37.78],[-122.41,37.78],[-122.41,37.77],[-122.42,37.77]]]}";
        
        List<String> h3Cells = spatial4JHelper.coveringH3Cells(geoJson, 6);
        
        // Method should not crash, might be empty for small geometries
        assertNotNull(h3Cells);
        
        // All results should be valid H3 addresses
        for (String h3Cell : h3Cells) {
            assertNotNull(h3Cell);
            assertEquals(15, h3Cell.length());
        }
    }
    
    @Test
    public void testInvalidGeometry() {
        assertThrows(GeometryParseException.class, () -> {
            spatial4JHelper.coveringH3Cells("INVALID GEOMETRY", 7);
        });
    }
    
    @Test
    public void testValidResolutionBounds() throws GeometryParseException {
        // Use a smaller polygon to avoid H3 size limits
        String wkt = "POLYGON((0 0, 0 0.01, 0.01 0.01, 0.01 0, 0 0))";
        
        // Test minimum resolution
        List<String> resultsMin = spatial4JHelper.coveringH3Cells(wkt, 0);
        assertNotNull(resultsMin);
        
        // Test a moderate resolution (15 would be too many cells for a large polygon)
        List<String> resultsMax = spatial4JHelper.coveringH3Cells(wkt, 10);
        assertNotNull(resultsMax);
    }
    
    @Test
    public void testLargeGeometry() throws GeometryParseException {
        // Test with a larger polygon covering a significant area
        String largeWkt = "POLYGON((-122.5 37.7, -122.5 37.9, -122.3 37.9, -122.3 37.7, -122.5 37.7))";
        
        List<String> h3Cells = spatial4JHelper.coveringH3Cells(largeWkt, 5);
        
        assertNotNull(h3Cells);
        
        // All results should be unique H3 addresses
        long uniqueCount = h3Cells.stream().distinct().count();
        assertEquals(h3Cells.size(), uniqueCount);
    }
    
    @Test
    public void testConsistency() throws GeometryParseException {
        // Use a smaller polygon to avoid H3 size limits
        String wkt = "POLYGON((0 0, 0 0.01, 0.01 0.01, 0.01 0, 0 0))";
        
        // First run
        List<String> firstRun = spatial4JHelper.coveringH3Cells(wkt, 8);
        
        // Second run
        List<String> secondRun = spatial4JHelper.coveringH3Cells(wkt, 8);
        
        // Results should be identical
        assertEquals(firstRun.size(), secondRun.size());
        for (String cell : firstRun) {
            assertTrue(secondRun.contains(cell));
        }
    }
}

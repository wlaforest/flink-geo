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

package com.github.wlaforest.geo;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class H3UtilsTest {
    
    private H3Utils h3Utils;
    private GeometryFactory geometryFactory;
    
    @BeforeEach
    public void setUp() throws Exception {
        h3Utils = new H3Utils();
        geometryFactory = new GeometryFactory();
    }
    
    @Test
    public void testLatLonToCellWithResolution() {
        double lat = 37.7749;
        double lon = -122.4194;
        
        String h3Cell = h3Utils.latLonToCell(lat, lon, 7);
        
        assertNotNull(h3Cell);
        assertEquals(15, h3Cell.length());
        assertTrue(h3Utils.isValidCell(h3Cell));
        assertEquals(7, h3Utils.getResolution(h3Cell));
    }
    
    @Test
    public void testLatLonToCellWithDefaultResolution() {
        double lat = 40.7128;
        double lon = -74.0060;
        
        String h3Cell = h3Utils.latLonToCell(lat, lon);
        
        assertNotNull(h3Cell);
        assertEquals(15, h3Cell.length());
        assertTrue(h3Utils.isValidCell(h3Cell));
        assertEquals(7, h3Utils.getResolution(h3Cell)); // Default resolution is 7
    }
    
    @Test
    public void testCellToLatLon() {
        double lat = 37.7749;
        double lon = -122.4194;
        
        String h3Cell = h3Utils.latLonToCell(lat, lon, 8);
        double[] centerCoords = h3Utils.cellToLatLon(h3Cell);
        
        assertNotNull(centerCoords);
        assertEquals(2, centerCoords.length);
        
        // The center should be close to the original coordinates
        double latDiff = Math.abs(centerCoords[0] - lat);
        double lonDiff = Math.abs(centerCoords[1] - lon);
        
        // Tolerance depends on resolution - resolution 8 should be quite precise
        assertTrue(latDiff < 0.01);
        assertTrue(lonDiff < 0.01);
    }
    
    @Test
    public void testInvalidResolution() {
        double lat = 37.7749;
        double lon = -122.4194;
        
        // Test negative resolution
        assertThrows(IllegalArgumentException.class, () -> {
            h3Utils.latLonToCell(lat, lon, -1);
        });
        
        // Test too high resolution
        assertThrows(IllegalArgumentException.class, () -> {
            h3Utils.latLonToCell(lat, lon, 16);
        });
    }
    
    @Test
    public void testPolygonToCells() {
        // Create a small square polygon
        Coordinate[] coords = new Coordinate[] {
            new Coordinate(-122.42, 37.77),
            new Coordinate(-122.41, 37.77),
            new Coordinate(-122.41, 37.78),
            new Coordinate(-122.42, 37.78),
            new Coordinate(-122.42, 37.77)  // Close the ring
        };
        
        LinearRing ring = geometryFactory.createLinearRing(coords);
        Polygon polygon = geometryFactory.createPolygon(ring);
        
        List<String> cells = h3Utils.polygonToCells(polygon, 7);
        
        assertNotNull(cells);
        assertFalse(cells.isEmpty());
        
        // All cells should be valid and have the correct resolution
        for (String cell : cells) {
            assertTrue(h3Utils.isValidCell(cell));
            assertEquals(7, h3Utils.getResolution(cell));
        }
    }
    
    @Test
    public void testBoundingBoxToCells() {
        double minLat = 37.77;
        double minLon = -122.42;
        double maxLat = 37.78;
        double maxLon = -122.41;
        
        List<String> cells = h3Utils.boundingBoxToCells(minLat, minLon, maxLat, maxLon, 6);
        
        assertNotNull(cells);
        assertFalse(cells.isEmpty());
        
        // All cells should be valid and have the correct resolution
        for (String cell : cells) {
            assertTrue(h3Utils.isValidCell(cell));
            assertEquals(6, h3Utils.getResolution(cell));
        }
    }
    
    @Test
    public void testResolutionDifferences() {
        double lat = 37.7749;
        double lon = -122.4194;
        
        // Test different resolutions produce different results
        String h3Res0 = h3Utils.latLonToCell(lat, lon, 0);
        String h3Res5 = h3Utils.latLonToCell(lat, lon, 5);
        String h3Res10 = h3Utils.latLonToCell(lat, lon, 10);
        String h3Res15 = h3Utils.latLonToCell(lat, lon, 15);
        
        // All should be different
        assertNotEquals(h3Res0, h3Res5);
        assertNotEquals(h3Res5, h3Res10);
        assertNotEquals(h3Res10, h3Res15);
        
        // All should be valid
        assertTrue(h3Utils.isValidCell(h3Res0));
        assertTrue(h3Utils.isValidCell(h3Res5));
        assertTrue(h3Utils.isValidCell(h3Res10));
        assertTrue(h3Utils.isValidCell(h3Res15));
        
        // Resolutions should be correct
        assertEquals(0, h3Utils.getResolution(h3Res0));
        assertEquals(5, h3Utils.getResolution(h3Res5));
        assertEquals(10, h3Utils.getResolution(h3Res10));
        assertEquals(15, h3Utils.getResolution(h3Res15));
    }
    
    @Test
    public void testIsValidCell() {
        // Test with a valid H3 cell
        String validCell = h3Utils.latLonToCell(37.7749, -122.4194, 7);
        assertTrue(h3Utils.isValidCell(validCell));
        
        // Test with invalid cells
        assertFalse(h3Utils.isValidCell("invalid"));
        assertFalse(h3Utils.isValidCell("123456789012345"));
        assertFalse(h3Utils.isValidCell(""));
        assertFalse(h3Utils.isValidCell("zzzzzzzzzzzzzzz"));
    }
    
    @Test
    public void testConsistency() {
        double lat = 40.7128;
        double lon = -74.0060;
        int resolution = 8;
        
        // Multiple calls should return the same result
        String cell1 = h3Utils.latLonToCell(lat, lon, resolution);
        String cell2 = h3Utils.latLonToCell(lat, lon, resolution);
        
        assertEquals(cell1, cell2);
    }
    
    @Test
    public void testBoundaryCoordinates() {
        // Test at various boundary conditions
        
        // North pole
        String northPole = h3Utils.latLonToCell(90.0, 0.0, 5);
        assertTrue(h3Utils.isValidCell(northPole));
        
        // South pole
        String southPole = h3Utils.latLonToCell(-90.0, 0.0, 5);
        assertTrue(h3Utils.isValidCell(southPole));
        
        // International date line
        String east180 = h3Utils.latLonToCell(0.0, 180.0, 5);
        String west180 = h3Utils.latLonToCell(0.0, -180.0, 5);
        assertTrue(h3Utils.isValidCell(east180));
        assertTrue(h3Utils.isValidCell(west180));
        
        // These should be the same cell (180 and -180 longitude are the same)
        assertEquals(east180, west180);
    }
}

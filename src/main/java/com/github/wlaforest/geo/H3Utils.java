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

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for H3 operations.
 * @author Will LaForest
 */
public class H3Utils {
    
    private final H3Core h3;
    
    public H3Utils() throws IOException {
        this.h3 = H3Core.newInstance();
    }
    
    /**
     * Get the H3 cell ID for a given latitude and longitude at the specified resolution.
     * 
     * @param lat latitude
     * @param lon longitude
     * @param resolution H3 resolution (0-15)
     * @return H3 cell ID as a string
     */
    public String latLonToCell(double lat, double lon, int resolution) {
        if (resolution < 0 || resolution > 15) {
            throw new IllegalArgumentException("H3 resolution must be between 0 and 15");
        }
        return h3.latLngToCellAddress(lat, lon, resolution);
    }
    
    /**
     * Get the H3 cell ID for a given latitude and longitude using default resolution 7.
     * 
     * @param lat latitude
     * @param lon longitude
     * @return H3 cell ID as a string
     */
    public String latLonToCell(double lat, double lon) {
        return latLonToCell(lat, lon, 7);
    }
    
    /**
     * Get the center coordinates of an H3 cell.
     * 
     * @param h3Address H3 cell address
     * @return array of [lat, lon]
     */
    public double[] cellToLatLon(String h3Address) {
        LatLng center = h3.cellToLatLng(h3Address);
        return new double[]{center.lat, center.lng};
    }
    
    /**
     * Get all H3 cells that cover a given polygon.
     * 
     * @param polygon JTS Polygon geometry
     * @param resolution H3 resolution (0-15)
     * @return list of H3 cell addresses
     */
    public List<String> polygonToCells(Polygon polygon, int resolution) {
        if (resolution < 0 || resolution > 15) {
            throw new IllegalArgumentException("H3 resolution must be between 0 and 15");
        }
        
        List<LatLng> boundary = new ArrayList<>();
        Coordinate[] coords = polygon.getExteriorRing().getCoordinates();
        
        // Convert coordinates to H3 LatLng format
        for (int i = 0; i < coords.length - 1; i++) { // -1 to skip the duplicate closing coordinate
            boundary.add(new LatLng(coords[i].y, coords[i].x));
        }
        
        // Get all cells that intersect with the polygon
        List<String> cells = h3.polygonToCellAddresses(boundary, null, resolution);
        return cells;
    }
    
    /**
     * Get H3 cells covering a bounding box.
     * 
     * @param minLat minimum latitude
     * @param minLon minimum longitude
     * @param maxLat maximum latitude
     * @param maxLon maximum longitude
     * @param resolution H3 resolution (0-15)
     * @return list of H3 cell addresses
     */
    public List<String> boundingBoxToCells(double minLat, double minLon, double maxLat, double maxLon, int resolution) {
        if (resolution < 0 || resolution > 15) {
            throw new IllegalArgumentException("H3 resolution must be between 0 and 15");
        }
        
        List<LatLng> boundary = new ArrayList<>();
        boundary.add(new LatLng(minLat, minLon));
        boundary.add(new LatLng(minLat, maxLon));
        boundary.add(new LatLng(maxLat, maxLon));
        boundary.add(new LatLng(maxLat, minLon));
        
        List<String> cells = h3.polygonToCellAddresses(boundary, null, resolution);
        return cells;
    }
    
    /**
     * Get the resolution of an H3 cell.
     * 
     * @param h3Address H3 cell address
     * @return resolution level (0-15)
     */
    public int getResolution(String h3Address) {
        return h3.getResolution(h3Address);
    }
    
    /**
     * Check if an H3 address is valid.
     * 
     * @param h3Address H3 cell address to validate
     * @return true if valid, false otherwise
     */
    public boolean isValidCell(String h3Address) {
        if (h3Address == null || h3Address.isEmpty()) {
            return false;
        }
        
        try {
            return h3.isValidCell(h3Address);
        } catch (Exception e) {
            // If any exception occurs during validation, the cell is invalid
            return false;
        }
    }
}

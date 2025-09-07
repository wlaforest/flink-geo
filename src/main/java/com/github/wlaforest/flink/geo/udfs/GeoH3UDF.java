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

import com.github.wlaforest.geo.H3Utils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink UDF to calculate the H3 cell ID of a given point.
 * Based on Uber's H3 hexagonal hierarchical spatial index.
 * 
 * Usage in Flink SQL:
 * CREATE FUNCTION geo_h3 AS 'com.github.wlaforest.flink.geo.udfs.GeoH3UDF';
 * 
 * Examples:
 * SELECT geo_h3(lat, lon) FROM table;          -- defaults to resolution 7
 * SELECT geo_h3(lat, lon, 10) FROM table;      -- specify resolution
 */
public class GeoH3UDF extends ScalarFunction {
    
    private transient H3Utils h3Utils;
    
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        h3Utils = new H3Utils();
    }
    
    /**
     * Function to calculate the H3 cell ID of a given point. Resolution defaults to 7
     * 
     * @param lat latitude
     * @param lon longitude
     * @return H3 cell ID as string
     */
    public String eval(Double lat, Double lon) {
        if (lat == null || lon == null) {
            return null;
        }
        
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            throw new IllegalArgumentException("lat or lon are out of boundaries for -90 to 90 and -180 to 180");
        }
        
        return h3Utils.latLonToCell(lat, lon);
    }
    
    /**
     * Function to calculate the H3 cell ID of a given point with specified resolution
     * 
     * @param lat latitude
     * @param lon longitude
     * @param resolution H3 resolution level (0-15)
     * @return H3 cell ID as string
     */
    public String eval(Double lat, Double lon, Integer resolution) {
        if (lat == null || lon == null || resolution == null) {
            return null;
        }
        
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            throw new IllegalArgumentException("lat or lon are out of boundaries for -90 to 90 and -180 to 180");
        }
        
        if (resolution < 0 || resolution > 15) {
            throw new IllegalArgumentException("H3 resolution must be between 0 and 15");
        }
        
        return h3Utils.latLonToCell(lat, lon, resolution);
    }
    
    @Override
    public boolean isDeterministic() {
        return true;
    }
}

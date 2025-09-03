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

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.spatial4j.io.GeohashUtils;

/**
 * Flink UDF to calculate the geohash of a given point.
 * Based on the Lucene code https://lucene.apache.org/core/5_5_0/spatial/org/apache/lucene/spatial/util/GeoHashUtils.html
 * 
 * Usage in Flink SQL:
 * CREATE FUNCTION geo_geohash AS 'com.github.wlaforest.flink.geo.udfs.GeoHashUDF';
 * 
 * Examples:
 * SELECT geo_geohash(lat, lon) FROM table;  -- defaults to precision 12
 * SELECT geo_geohash(lat, lon, 7) FROM table;  -- specify precision
 */
public class GeoHashUDF extends ScalarFunction {
    
    /**
     * Function to calculate the geohash of a given point. Precision defaults to 12
     * 
     * @param lat latitude
     * @param lon longitude
     * @return geohash string
     */
    public String eval(Double lat, Double lon) {
        if (lat == null || lon == null) {
            return null;
        }
        
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            throw new IllegalArgumentException("lat or lon are out of boundaries for -90 to 90 and -180 to 180");
        }
        
        return GeohashUtils.encodeLatLon(lat, lon);
    }
    
    /**
     * Function to calculate the geohash of a given point with specified precision
     * 
     * @param lat latitude
     * @param lon longitude
     * @param precision what level of precision? Goes from 1-12
     * @return geohash string
     */
    public String eval(Double lat, Double lon, Integer precision) {
        if (lat == null || lon == null || precision == null) {
            return null;
        }
        
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            throw new IllegalArgumentException("lat or lon are out of boundaries for -90 to 90 and -180 to 180");
        }
        
        if (precision < 1 || precision > 12) {
            throw new IllegalArgumentException("Precision must be between 1 and 12");
        }
        
        return GeohashUtils.encodeLatLon(lat, lon, precision);
    }
    
    @Override
    public boolean isDeterministic() {
        return true;
    }
}

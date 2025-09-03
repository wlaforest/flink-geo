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
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Flink UDF function to test for geometry intersection in euclidean space.
 * Geometry encoded in WKT or GeoJSON. null value result in false being returned.
 * 
 * Usage in Flink SQL:
 * CREATE FUNCTION geo_intersected AS 'com.github.wlaforest.flink.geo.udfs.GeoIntersectedUDF';
 * SELECT geo_intersected(wkt_geom1, wkt_geom2) FROM table;
 */
@FunctionHint(
    output = @DataTypeHint("BOOLEAN"),
    input = {@DataTypeHint("STRING"), @DataTypeHint("STRING")}
)
public class GeoIntersectedUDF extends ScalarFunction {
    
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
     * Determines if the two geometries intersect.
     * 
     * @param geo1 WKT or GeoJSON Encoded Geometry to check for intersection with geo2
     * @param geo2 WKT or GeoJSON Encoded Geometry to check for intersection with geo1
     * @return true if the geometries intersect, false otherwise
     */
    public Boolean eval(String geo1, String geo2) {
        if (geo1 == null || geo2 == null) {
            return false;
        }
        
        try {
            return spatial4JHelper.intersect(geo1, geo2);
        } catch (GeometryParseException e) {
            throw new RuntimeException("Failed to parse geometry: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean isDeterministic() {
        return true;
    }
}

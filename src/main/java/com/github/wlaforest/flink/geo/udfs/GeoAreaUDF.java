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

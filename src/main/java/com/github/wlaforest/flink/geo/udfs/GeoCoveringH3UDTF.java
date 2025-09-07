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
import org.apache.flink.table.functions.TableFunction;

import java.util.List;

/**
 * Flink UDTF for computing the H3 cells to completely cover a geometry.
 * It will calculate all the H3 cells a geometry falls in.
 * This is very useful for partitioning for the distributed join
 * 
 * Usage in Flink SQL:
 * CREATE FUNCTION geo_covering_h3 AS 'com.github.wlaforest.flink.geo.udfs.GeoCoveringH3UDTF';
 * 
 * Example:
 * SELECT T.*, h3_cell 
 * FROM myTable T, 
 * LATERAL TABLE(geo_covering_h3(T.wkt_column)) AS L(h3_cell);
 * 
 * Or with resolution:
 * SELECT T.*, h3_cell 
 * FROM myTable T, 
 * LATERAL TABLE(geo_covering_h3(T.wkt_column, 7)) AS L(h3_cell);
 */
@FunctionHint(output = @DataTypeHint("STRING"))
public class GeoCoveringH3UDTF extends TableFunction<String> {
    
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
     * Takes WKT or GeoJSON Encoded Geometry and an H3 resolution and computes all H3 cells
     * the geometry falls in. This is helpful for re-keying a stream
     * 
     * @param geo WKT or GeoJSON encoded geometry
     * @param resolution H3 resolution level (0-15)
     */
    public void eval(String geo, Integer resolution) {
        if (geo == null || resolution == null) {
            return;
        }
        
        if (resolution < 0 || resolution > 15) {
            throw new IllegalArgumentException("H3 resolution must be between 0 and 15");
        }
        
        try {
            List<String> h3Cells = spatial4JHelper.coveringH3Cells(geo, resolution);
            for (String h3Cell : h3Cells) {
                collect(h3Cell);
            }
        } catch (GeometryParseException e) {
            throw new RuntimeException("Failed to parse geometry: " + e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error in geo_covering_h3: " + e.getMessage(), e);
        }
    }
    
    /**
     * Takes WKT or GeoJSON Encoded Geometry and computes all H3 cells
     * the geometry falls in using default resolution of 7
     * 
     * @param geo WKT or GeoJSON encoded geometry
     */
    public void eval(String geo) {
        eval(geo, 7);
    }
}

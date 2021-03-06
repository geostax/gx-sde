/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2009, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package com.geostax.sde.postgis;

import static com.geostax.sde.postgis.PostgisNGDataStoreFactory.*;

import java.util.Map;

import com.geostax.sde.jdbc.JDBCJNDIDataStoreFactory;

/**
 * JNDI DataStoreFactory  for Postgis database.
 * 
 * @author Christian Mueller
 * 
 *
 *
 *
 * @source $URL$
 */
public class PostgisNGJNDIDataStoreFactory extends JDBCJNDIDataStoreFactory {

    public PostgisNGJNDIDataStoreFactory() {
        super(new PostgisNGDataStoreFactory());
    }
    
    @Override
    protected void setupParameters(Map parameters) {
        super.setupParameters(parameters);
        
        parameters.put(LOOSEBBOX.key, LOOSEBBOX);
        parameters.put(ESTIMATED_EXTENTS.key, ESTIMATED_EXTENTS);
        parameters.put(PREPARED_STATEMENTS.key, PREPARED_STATEMENTS);
        parameters.put(ENCODE_FUNCTIONS.key, ENCODE_FUNCTIONS);
        parameters.put(SIMPLIFY.key, SIMPLIFY);
    }
}

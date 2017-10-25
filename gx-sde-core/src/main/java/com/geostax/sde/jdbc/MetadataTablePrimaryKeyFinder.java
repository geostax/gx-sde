/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2016, Open Source Geospatial Foundation (OSGeo)
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
package com.geostax.sde.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.util.logging.Logging;

/**
 * Looks up primary key information in a metadata table provided by the user.
 * <p>
 * The table schema will contain:
 * <ul>
 * <li>table_schema (varchar): schema name</li>
 * <li>table_name (varchar): table name</li>
 * <li>pk_column (varchar): column name</li>
 * <li>pk_column_idx (integer): column index if pk is multicolumn (nullable)</li>
 * <li>pk_policy (varchar): pk assignment policy: "assigned", "sequence", "autogenerated"</li>
 * <li>pk_sequence (varchar): full name of the sequence to be used to generate the next value, if
 * any</li>
 * </ul>
 * 
 * By default the table is named 'GT_PK_METADATA'.
 * 
 * @see #DEFAULT_TABLE
 * @author Andrea Aime - OpenGeo
 * 
 *
 *
 * @source $URL$
 */
public class MetadataTablePrimaryKeyFinder extends PrimaryKeyFinder {
    protected static final Logger LOGGER = Logging.getLogger(MetadataTablePrimaryKeyFinder.class);

    /**
     * The default metadata table name: {@value}.
     */
    public static final String DEFAULT_TABLE = "GT_PK_METADATA";
    
    volatile Boolean metadataTableExists = null; 

    /**
     * Known policies pk column treatment policies.
     */
    enum Policy {
        assigned, sequence, autogenerated
    };

    /**
     * The schema that will contain the metadata table.
     */
    String tableSchema;

    /**
     * The table that will contain the metadata information.
     */
    String tableName = DEFAULT_TABLE;

    /**
     * The schema containing the table schema
     */
    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
        this.metadataTableExists = null;
    }

    /**
     * The metadata table name, defaults to {@code GT_PK_METADATA} if not specified.
     * 
     * @see #DEFAULT_TABLE
     */
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
        this.metadataTableExists = null;
    }

    @Override
    public PrimaryKey getPrimaryKey(JDBCDataStore store, String schema, String table, Connection cx)
            throws SQLException {
        ResultSet rs = null;
        PreparedStatement st = null;
        
        String metadataSchema = getMetadataSchema(store);
        
        try {
            // first off, make sure the metadata table is there (we'll also
            // catch errors later but log them at a higher level in case the table
            // is there but does not have the required structure). We just don't want
            // to fill the logs of people not using the metadata table with errors 
            if(metadataTableExists == null) {
                synchronized (this) {
                    if(metadataTableExists == null) {
                        try {
                            
                            StringBuffer sb = new StringBuffer();
                            sb.append("SELECT * FROM ");
                            if (metadataSchema != null) {
                                store.getSQLDialect().encodeSchemaName(metadataSchema, sb);
                                sb.append(".");
                            }
                            sb.append(tableName).append(" WHERE 1 = 0");
                            st = cx.prepareStatement(sb.toString());
                            rs = st.executeQuery();
                            metadataTableExists = true;
                        } catch(Exception e) {
                            // clean up the transaction status in case we are in auto-commit mode
                            if (e instanceof SQLException && !cx.getAutoCommit()) {
                                cx.rollback();
                            }
                            metadataTableExists = false;
                        } finally {
                            store.closeSafe(rs);
                            store.closeSafe(st);
                        }
                    }
                }
            }
            if(!metadataTableExists) {
                return null;
            }
            
            // build query against the metadata table
            List<String> parameters = new ArrayList<String>();
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT * FROM ");
            if (metadataSchema != null) {
                store.getSQLDialect().encodeSchemaName(metadataSchema, sb);
                sb.append(".");
            }
            sb.append(tableName);
            sb.append(" WHERE ");
            if (schema != null) {
                sb.append("table_schema = ? AND ");
                parameters.add(schema);
            }
            sb.append("table_name = ?");
            parameters.add(table);
            sb.append(" ORDER BY ");
            sb.append("pk_column_idx");
            sb.append(" ASC");
            String sql = sb.toString();
            LOGGER.log(Level.FINE, "Reading metadata table metadata: {0} [ parameters = {1} ]", new Object[] { sql, parameters });

            // extract information column by column
            DatabaseMetaData metaData = cx.getMetaData();
            st = cx.prepareStatement(sql);
            for (int i = 0; i < parameters.size(); i++) {
                st.setString(i+1, parameters.get(i));
            }
            rs = st.executeQuery();

            List<PrimaryKeyColumn> columns = new ArrayList<PrimaryKeyColumn>();
            Set<String> colNames = null;
            while (rs.next()) {
                String colName = rs.getString("pk_column");
                String policyStr = rs.getString("pk_policy");
                String sequence = rs.getString("pk_sequence");
                
                // check the column name is known
                if(colNames == null) {
                    colNames = getColumnNames(store, metaData, schema, table);
                }
                if(!colNames.contains(colName)) {
                    LOGGER.warning("Unknown column " + colName + " in table " + table);
                    return null;
                }
                    

                Policy policy = Policy.assigned;
                if (policyStr != null) {
                    try {
                        policy = Policy.valueOf(policyStr.toLowerCase());
                    } catch (IllegalArgumentException e) {
                        LOGGER.warning("Invalid policy value " + policyStr + ", valid values are"
                                + Arrays.asList(Policy.values()));
                        return null;
                    }
                }

                Class columnType = store.getColumnType(metaData, schema, table, colName);

                if (policy == Policy.assigned) {
                    columns.add(new NonIncrementingPrimaryKeyColumn(colName, columnType));
                } else if (policy == Policy.autogenerated) {
                    columns.add(new AutoGeneratedPrimaryKeyColumn(colName, columnType));
                } else if (policy == policy.sequence) {
                    columns.add(new SequencedPrimaryKeyColumn(colName, columnType, sequence));
                }
            }
            
            // see if we accumulated any info about this table
            if(columns.size() > 0)
                return new PrimaryKey(table, columns);
            else
                return null;
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Errors occurred accessing the primary key metadata table ",
                    e);
            return null;
        } finally {
            store.closeSafe(rs);
            store.closeSafe(st);
        }
    }

    Set<String> getColumnNames(JDBCDataStore store, DatabaseMetaData metaData, String schema,
                               String table) throws SQLException {
        ResultSet rs = null;
        Set<String> result = new HashSet<String>();
        try {
            rs = metaData.getColumns(null, store.escapeNamePattern(metaData, schema),
                    store.escapeNamePattern(metaData, table), null);
            while(rs.next()) {
                result.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            rs.close();
        }
        
        return result;
    }
    
    String getMetadataSchema(JDBCDataStore store) {
        if(tableSchema != null)
            return tableSchema;
        
        return store.getDatabaseSchema();
    }

}

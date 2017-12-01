package com.geostax.cassandra;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataUtilities;
import org.geotools.data.Parameter;
import org.geotools.util.SimpleInternationalString;

public class CassandraDataStoreFactory implements DataStoreFactorySpi {

	/** parameter for database type */
	public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true, "cassandra");

	/** parameter for database host */
	public static final Param HOST = new Param("host", String.class, "Host", true, "localhost");

	/** parameter for database user */
	public static final Param USER = new Param("user", String.class, "user name to login as");

	/** parameter for database password */
	public static final Param PASSWD = new Param("passwd", String.class,
			new SimpleInternationalString("password used to login"), false, null,
			Collections.singletonMap(Parameter.IS_PASSWORD, Boolean.TRUE));

	/**
	 * Public "no argument" constructor called by Factory Service Provider (SPI)
	 * entry listed in META-INF/services/org.geotools.data.DataStoreFactorySPI
	 */
	public CassandraDataStoreFactory() {
	}

	/** No implementation hints required at this time */
	public Map<Key, ?> getImplementationHints() {
		return Collections.emptyMap();
	}

	public String getDescription() {
		return "Accumulo Data Store.";
	}

	/** Confirm DataStore availability, null if unknown */
	Boolean isAvailable = null;

	@Override
	public String getDisplayName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAvailable() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean canProcess(Map<String, Serializable> params) {
		if (!DataUtilities.canProcess(params, getParametersInfo())) {
			return false;
		}
		return checkDBType(params);
	}

	protected boolean checkDBType(Map<String, Serializable> params) {
		return true;
	}

	public Param[] getParametersInfo() {
		LinkedHashMap<String, Param> map = new LinkedHashMap<>();
		setupParameters(map);
		return (Param[]) map.values().toArray(new Param[map.size()]);
	}

	protected void setupParameters(Map<String, Param> parameters) {
		parameters.put(DBTYPE.key, DBTYPE);
		parameters.put(HOST.key, HOST);
		parameters.put(USER.key, USER);
		parameters.put(PASSWD.key, PASSWD);

	}

	protected String getDatabaseID() {
		return (String) DBTYPE.sample;
	}

	@Override
	public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
		CassandraDataStore datastore = new CassandraDataStore(params.get("host").toString());
		return datastore;
	}

	@Override
	public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
		return new CassandraDataStore(params.get(HOST).toString());
	}

}

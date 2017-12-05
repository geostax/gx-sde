package com.geostax.cassandra;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.datastax.driver.core.Session;
import com.geostax.sde.data.ContentEntry;
import com.geostax.sde.data.ContentFeatureSource;

public class CassandraFeatureSource extends ContentFeatureSource {

	Session session;

	/**
	 * Creates the new feature store.
	 * 
	 * @param entry
	 *            The datastore entry.
	 * @param query
	 *            The defining query.
	 */
	public CassandraFeatureSource(ContentEntry entry) {
		this(entry, Query.ALL);

	}

	public CassandraFeatureSource(ContentEntry entry, Query query) {
		super(entry, query);
	}
	
	@Override
	public CassandraDataStore getDataStore() {
		return (CassandraDataStore) super.getDataStore();
	}
	
	@Override
	protected SimpleFeatureType buildFeatureType() throws IOException {
		return getDataStore().getSchema(entry.getName());
	}
	
	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	protected int getCountInternal(Query query) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
		session=CassandraConnector.getSession();
		CassandraFeatureReader reader = new CassandraFeatureReader(session,getSchema(),query);
		return reader;
	}
}

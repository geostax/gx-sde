package com.geostax.cassandra;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.QueryCapabilities;
import org.geotools.data.ResourceInfo;
import org.geotools.data.Transaction;
import org.geotools.factory.Hints.Key;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import com.datastax.driver.core.Session;
import com.geostax.sde.data.ContentEntry;
import com.geostax.sde.data.ContentFeatureStore;
import com.geostax.sde.data.ContentState;

public class CassandraFeatureStore extends ContentFeatureStore {

	CassandraFeatureSource delegate;
	Session session = null;

	public CassandraFeatureStore(ContentEntry entry) {
		super(entry, Query.ALL);
		this.delegate = new CassandraFeatureSource(entry) {
			@Override
			public void setTransaction(Transaction transaction) {
				super.setTransaction(transaction);
				// keep this feature store in sync
				CassandraFeatureStore.this.setTransaction(transaction);
			}
		};

	}

	@Override
	protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(Query query, int flags)
			throws IOException {
		// TODO Auto-generated method stub
		session = CassandraConnector.getSession();
		session.execute("use "+delegate.getName().getNamespaceURI()+";");
		return new CassandraInsertFeatureWriter(delegate.getSchema(),delegate.getName().getLocalPart(), session);
		// return new CassandraInsertFeatureWriter(delegate.getSchema());
	}

	// METHODS DELEGATED TO OGRFeatureSource
	// ----------------------------------------------------------------------------------------

	public CassandraDataStore getDataStore() {
		return delegate.getDataStore();
	}

	public Transaction getTransaction() {
		return delegate.getTransaction();
	}

	public ResourceInfo getInfo() {
		return delegate.getInfo();
	}

	public QueryCapabilities getQueryCapabilities() {
		return delegate.getQueryCapabilities();
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		return delegate.getBoundsInternal(query);
	}

	@Override
	protected int getCountInternal(Query query) throws IOException {
		return delegate.getCountInternal(query);
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
		return delegate.getReaderInternal(query);
	}

	@Override
	protected SimpleFeatureType buildFeatureType() throws IOException {
		return delegate.buildFeatureType();
	}

	@Override
	public ContentEntry getEntry() {
		return delegate.getEntry();
	}

	@Override
	public Name getName() {
		return delegate.getName();
	}

	@Override
	public ContentState getState() {
		return delegate.getState();
	}

	@Override
	public void setTransaction(Transaction transaction) {
		super.setTransaction(transaction);

		if (delegate.getTransaction() != transaction) {
			delegate.setTransaction(transaction);
		}
	}

}

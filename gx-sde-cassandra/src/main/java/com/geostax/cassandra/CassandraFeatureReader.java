package com.geostax.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.datastax.driver.core.Session;
import com.vividsolutions.jts.geom.Envelope;

public class CassandraFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

	CassandraQueryManagement manager;
	SimpleFeature currentFeature;
	Iterator<SimpleFeature> itr;
	
	Envelope bbox;
	Query query;
	SimpleFeatureType sft;
	String schema_name="";
	public CassandraFeatureReader(Session session, SimpleFeatureType sft, Query query) {
		this.sft = sft;
		this.query=query;
		schema_name=sft.getName().getNamespaceURI()+"."+sft.getName().getLocalPart();
		manager=new CassandraQueryManagement(session, sft, query);
		try {
			fetch();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void fetch() throws Exception {
		bbox = new ReferencedEnvelope();

		if (query.getFilter() != null) {
			bbox = (Envelope) query.getFilter().accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, bbox);
			if (bbox == null) {
				bbox = new ReferencedEnvelope();
			}
		}
		StringBuilder sb = new StringBuilder();
		sb.append(bbox.getMinY() + ":" + bbox.getMinX() + ",");
		sb.append(bbox.getMinY() + ":" + bbox.getMaxX() + ",");
		sb.append(bbox.getMaxY() + ":" + bbox.getMaxX() + ",");
		sb.append(bbox.getMaxY() + ":" + bbox.getMinX() + ";");
		String polygon = sb.toString();

		double lat0 = bbox.getMinY();
		double lon0 = bbox.getMinX();
		double lat1 = bbox.getMaxY();
		double lon1 = bbox.getMaxX();
		
		manager.queryData(schema_name, lat0, lon0, lat1, lon1);

	}

	@Override
	public SimpleFeatureType getFeatureType() {
		return sft;
	}

	@Override
	public boolean hasNext() throws IOException {
		return itr.hasNext();
	}

	@Override
	public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
		// TODO Auto-generated method stub
		currentFeature = itr.next();
		return currentFeature;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}
}

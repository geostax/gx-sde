package com.geostax.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.geotools.data.Query;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.geostax.cassandra.index.S2Index;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2RegionCoverer;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;

public class CassandraQueryManagement {

	public final static int CELL_LEVEL = 10;
	private Cluster cluster = null;
	private S2Index s2index;
	private List<SimpleFeature> result;
	private Set<String> idset;
	private List<String> idset2;
	private ExecutorService executorService;
	private Session session;
	private SimpleFeatureType sft;
	Query query;
	
	public CassandraQueryManagement(Session session, SimpleFeatureType sft, Query query) {
		this.session=session;
		this.sft=sft;
		this.query=query;
		s2index=new S2Index();
		
		result = Collections.synchronizedList(new ArrayList<>());
		executorService = Executors.newFixedThreadPool(5);
		idset = Collections.synchronizedSet(new TreeSet<String>());
		idset2 = Collections.synchronizedList(new ArrayList<>());
	}
	
			
	public void queryData(String schema_name,double lat0,double lon0,double lat1,double lon1) {
		String polygon = lat0 + ":" + lon0 + "," + lat0 + ":" + lon1 + "," + lat1 + ":" + lon1 + "," + lat1 + ":" + lon0
				+ ";";
		System.out.println(schema_name);
		System.out.println(polygon);
		List<String> quad_ids = new ArrayList<>();

		S2RegionCoverer coverer = new S2RegionCoverer();
		S2Polygon a = s2index.makePolygon(polygon);
		coverer.setMinLevel(CELL_LEVEL);
		coverer.setMaxLevel(CELL_LEVEL);
		ArrayList<S2CellId> covering = new ArrayList<>();
		coverer.getCovering(a, covering);
		System.out.println(covering.size());

		// System.out.println(sft);
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

		ArrayList<SimpleFeature> features = new ArrayList<>();
		Envelope bbox = new ReferencedEnvelope(lon0, lon1, lat0, lat1, DefaultGeographicCRS.WGS84);
		for (S2CellId id : covering) {

			Statement statement = new SimpleStatement("select * from " + schema_name + " where cell_id=?;",
					id.toToken());
			executorService.submit(new QueryProcess(session, statement, builder, bbox));
		}
		executorService.shutdown();

		while (true) {
			if (executorService.isTerminated()) {
				System.out.println("所有的子线程都结束了！");
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		System.out.println(result.size());
		System.out.println(idset.size());
	}
	public void queryData(String schema_name) {

		long t0 = System.currentTimeMillis();
		// double lat0 = 36.958;
		// double lon0 = 138.217;
		// double lat1 = 34.252;
		// double lon1 = 140.985;

		// double lat0 = 38;
		// double lon0 = 138;
		// double lat1 = 34.5;
		// double lon1 = 141.2;

		// double lat0 = 33.4;
		// double lon0 = 135;
		// double lat1 = 40.4;
		// double lon1 = 142.0;

		double lat0 = 29;
		double lon0 = 127;
		double lat1 = 46;
		double lon1 = 148.0;

		String polygon = lat0 + ":" + lon0 + "," + lat0 + ":" + lon1 + "," + lat1 + ":" + lon1 + "," + lat1 + ":" + lon0
				+ ";";
		List<String> quad_ids = new ArrayList<>();

		S2RegionCoverer coverer = new S2RegionCoverer();
		S2Polygon a = s2index.makePolygon(polygon);
		coverer.setMinLevel(CELL_LEVEL);
		coverer.setMaxLevel(CELL_LEVEL);
		ArrayList<S2CellId> covering = new ArrayList<>();
		coverer.getCovering(a, covering);
		System.out.println(covering.size());
		Session session = cluster.connect();
		session.execute("use japan;");

		// System.out.println(sft);
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

		ArrayList<SimpleFeature> features = new ArrayList<>();
		Envelope bbox = new ReferencedEnvelope(lon0, lon1, lat0, lat1, DefaultGeographicCRS.WGS84);
		for (S2CellId id : covering) {

			Statement statement = new SimpleStatement("select * from " + schema_name + " where cell_id=?;",
					id.toToken());
			executorService.submit(new QueryProcess(session, statement, builder, bbox));
		}
		executorService.shutdown();

		while (true) {
			if (executorService.isTerminated()) {
				System.out.println("所有的子线程都结束了！");
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		System.out.println((System.currentTimeMillis() - t0) + " ms");
		System.out.println(result.size());
		System.out.println(idset.size());
	}

	class QueryProcess implements Runnable {

		Session session;
		String datetime;
		Geometry geometry;
		Statement statement;
		ByteBuffer buffer;
		SimpleFeatureBuilder builder;
		WKBReader reader = new WKBReader();
		Envelope bbox;

		public QueryProcess(Session session, Statement statement, SimpleFeatureBuilder builder, Envelope bbox) {
			this.session = session;
			this.statement = statement;
			this.builder = builder;
			this.bbox = bbox;
		}

		@Override
		public void run() {
			// System.out.println(statement);
			ResultSet rs = session.execute(statement);
			for (Row row : rs) {
				buffer = row.getBytes("the_geom");
				String fid = row.getString("osm_id");
				int partition = row.getInt("partition");
				if (idset2.contains(fid))
					continue;
				if (partition > 1) {
					idset2.add(fid);
				}

				try {
					geometry = reader.read(buffer.array());
					if (!bbox.intersects(geometry.getEnvelopeInternal())) {
						continue;
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				builder.set("the_geom", geometry);
				SimpleFeature feature = builder.buildFeature(fid);
				result.add(feature);
				idset.add(fid);
			}

		}

	}

}

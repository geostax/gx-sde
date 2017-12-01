package com.geostax.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.geostax.cassandra.index.S2Index;
import com.geostax.sde.shapefile.ShapefileDataStore;
import com.geostax.sde.shapefile.ShapefileDataStoreFactory;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2RegionCoverer;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

public class CassandraIngestTest {

	public final static int CELL_LEVEL = 5;
	public final static Map<Class, DataType> TYPE_TO_CA_MAP = new HashMap<Class, DataType>() {
		{
			put(Integer.class, DataType.cint());
			put(String.class, DataType.text());
			put(java.lang.Long.class, DataType.bigint());
			put(java.lang.Float.class, DataType.cfloat());
			put(java.lang.Double.class, DataType.cdouble());
			put(Date.class, DataType.timestamp());
			put(UUID.class, DataType.uuid());
			put(com.vividsolutions.jts.geom.Geometry.class, DataType.blob());
			put(Point.class, DataType.blob());
			put(MultiPolygon.class, DataType.blob());
			put(MultiLineString.class, DataType.blob());
		}
	};

	public final static Map<DataType, Class> CA_MAP_TO_TYPE = new HashMap<DataType, Class>() {
		{
			put(DataType.cint(), Integer.class);
			put(DataType.text(), String.class);
			put(DataType.bigint(), java.lang.Long.class);
			put(DataType.cfloat(), java.lang.Float.class);
			put(DataType.cdouble(), java.lang.Double.class);
			put(DataType.timestamp(), Date.class);
			put(DataType.uuid(), UUID.class);
			put(DataType.blob(), Geometry.class);
		}
	};
	private Cluster cluster = null;
	private S2Index s2index;
	private List<SimpleFeature> result;
	private Set<String> idset;
	private List<String> idset2;
	private ExecutorService executorService;

	public CassandraIngestTest() {
		cluster = Cluster.builder().addContactPoint("192.168.210.110").build();
		s2index = new S2Index();
		result = Collections.synchronizedList(new ArrayList<>());
		executorService = Executors.newFixedThreadPool(5);
		idset = Collections.synchronizedSet(new TreeSet<String>());
		idset2 = Collections.synchronizedList(new ArrayList<>());
	}

	public static void main(String[] args) throws Exception {
		new CassandraIngestTest().ingestData();
		//new CassandraIngestTest().queryData("gis_osm_buildings_a_free_1_l" + CELL_LEVEL);
	}

	public void ingestData() throws Exception {
		Session session = cluster.connect();

		String datetime = "2016121300";
		ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
		ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory
				.createDataStore(new File("E:\\Data\\OSM\\Japan\\japan-161213-free.shp\\gis.osm_pois_free_1.shp")
						.toURI().toURL());
		sds.setCharset(Charset.forName("GBK"));
		SimpleFeatureSource featureSource = sds.getFeatureSource();
		SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
		session.execute("use japan;");
		createSchema(session, featureType, "L" + CELL_LEVEL);
		System.out.println("Create Schema!");
		SimpleFeatureCollection featureCollection = featureSource.getFeatures();
		FeatureIterator<SimpleFeature> features = featureCollection.features();
		WKBWriter writer = new WKBWriter();
		int count = 0;
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);

		BatchStatement bs = new BatchStatement();
		String table_name = featureType.getName().toString().replace(".", "_") + "_L" + CELL_LEVEL;
		Geometry geom;
		int sum = 0;
		while (features.hasNext()) {
			SimpleFeature feature = features.next();
			if (featureType.getGeometryDescriptor().getType().getName().toString().equals("MultiPolygon")) {
				geom = (MultiPolygon) feature.getDefaultGeometry();
			} else if (featureType.getGeometryDescriptor().getType().getName().toString().equals("MultiLineString")) {
				geom = (MultiLineString) feature.getDefaultGeometry();
			} else if (featureType.getGeometryDescriptor().getType().getName().toString().equals("Polygon")) {
				geom = (Polygon) feature.getDefaultGeometry();
				System.out.println(geom);

			} else {
				geom = (Point) feature.getDefaultGeometry();
			}

			ByteBuffer buf_geom = ByteBuffer.wrap(writer.write(geom));

			List<S2CellId> ids = s2index.index(CELL_LEVEL, geom);
			if (ids.size() == 0 || ids.size() > 1000) {
				System.out.println(feature.getAttribute("osm_id"));
				continue;
			}

			if (ids.size() > 1)
				sum++;

			List<AttributeDescriptor> attrDes = featureType.getAttributeDescriptors();
			String cols = "";
			String params = "";
			for (S2CellId id : ids) {

				cols = "";
				params = "";

				List<String> col_items = new ArrayList<>();
				List<Object> values = new ArrayList<>();

				String fid = UUID.randomUUID().toString();

				values.add(id.toToken());
				values.add(date);
				values.add(fid);
				values.add(ids.size());

				for (AttributeDescriptor attr : attrDes) {
					if (attr instanceof GeometryDescriptor) {
						String col_name = attr.getLocalName();
						Class type = attr.getType().getBinding();
						col_items.add(col_name);
						values.add(buf_geom);
					} else {
						String col_name = attr.getLocalName();
						Class type = attr.getType().getBinding();
						col_items.add(col_name);
						values.add(feature.getAttribute(col_name));
					}

				}

				for (int i = 0; i < col_items.size() - 1; i++) {
					cols += col_items.get(i) + ",";
					params += "?,";
				}
				cols += col_items.get(col_items.size() - 1);
				params += "?";

				SimpleStatement s = new SimpleStatement("INSERT INTO " + table_name + " (cell_id,time,fid,partition,"
						+ cols + ") values (?,?,?,?," + params + ");", values.toArray());
				// System.out.println(s);
				bs.add(s);
				count++;
				if (count == 50) {
					try {
						session.execute(bs);
						bs.clear();
						count = 0;
					} catch (Exception e) {
						for (Statement stat : bs.getStatements()) {
							while (true) {
								try {
									session.execute(stat);
									break;
								} catch (Exception e2) {
									System.out.print("-");
								}
							}
							System.out.print(".");
						}
						System.out.print("\n");
						bs.clear();
						count = 0;
					}

				}
			}
		}

		session.execute(bs);

		System.out.println(sum);
		long t0 = System.currentTimeMillis();
		System.out.println("Finish!");

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

		SimpleFeatureType sft = getSchema(new NameImpl(schema_name),
				cluster.getMetadata().getKeyspace("japan").getTable(schema_name));
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

	private void createSchema(Session session, SimpleFeatureType featureType, String suffix) throws IOException {

		List<AttributeDescriptor> attrDes = featureType.getAttributeDescriptors();
		List<String> col_items = new ArrayList<>();
		for (AttributeDescriptor attr : attrDes) {
			String col_name = attr.getLocalName();
			Class type = attr.getType().getBinding();
			col_items.add(col_name + " " + CassandraIngestTest.TYPE_TO_CA_MAP.get(type).getName().toString());
		}
		String cols = "";
		for (int i = 0; i < col_items.size() - 1; i++) {
			cols += col_items.get(i) + ",";
		}
		cols += col_items.get(col_items.size() - 1);
		String colCreate = "(cell_id text,time timestamp,fid text,partition int," + cols
				+ ", PRIMARY KEY (cell_id,time,fid));";
		String stmt = "CREATE TABLE IF NOT EXISTS " + featureType.getName().toString().replace(".", "_") + "_" + suffix
				+ " " + colCreate;
		session.execute(stmt);

	}

	public SimpleFeatureType getSchema(Name name, TableMetadata table) {
		List<ColumnMetadata> columns = table.getColumns();
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(name);
		AttributeTypeBuilder attrTypeBuilder = new AttributeTypeBuilder();
		for (ColumnMetadata cm : columns) {
			String cname = cm.getName();
			Class binding = CA_MAP_TO_TYPE.get(cm.getType());
			if (!cm.getName().equals("cell_id") && !cm.getName().equals("time") && !cm.getName().equals("fid")) {
				if (Geometry.class.isAssignableFrom(binding)) {
					attrTypeBuilder.binding(binding);
					CoordinateReferenceSystem wsg84 = null;
					try {
						wsg84 = CRS.decode("EPSG:4326");
					} catch (Exception e) {
						e.printStackTrace();
					}
					attrTypeBuilder.setCRS(wsg84);
					builder.add(attrTypeBuilder.buildDescriptor(cname, attrTypeBuilder.buildGeometryType()));
				} else {
					builder.add(attrTypeBuilder.binding(binding).nillable(false).buildDescriptor(cname));
				}
			}
		}
		return builder.buildFeatureType();
	}
}

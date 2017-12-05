package com.geostax.cassandra;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.text.cql2.CQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.geostax.sde.shapefile.ShapefileDataStore;
import com.geostax.sde.shapefile.ShapefileDataStoreFactory;
import com.vividsolutions.jts.geom.Geometry;

public class CassandraDataStoreTest {

	public static void main(String[] args) throws Exception {
		new CassandraDataStoreTest().testReader();
	}

	public void testDataStore() throws Exception {
		Map<String, Serializable> params = new HashMap<>();

		params.put(CassandraDataStoreFactory.DBTYPE.key, "cassandra");
		params.put(CassandraDataStoreFactory.HOST.key, "192.168.210.110");
		params.put(CassandraDataStoreFactory.USER.key, "cassandra");
		params.put(CassandraDataStoreFactory.PASSWD.key, "cassandra");

		Iterator<DataStoreFactorySpi> iterators = DataStoreFinder.getAllDataStores();
		while (iterators.hasNext()) {
			DataStoreFactorySpi spi = iterators.next();
			if (spi.canProcess(params)) {
				System.out.println(spi);
			}
		}
		CassandraDataStoreFactory spi = new CassandraDataStoreFactory();
		CassandraDataStore store = (CassandraDataStore) spi.createDataStore(params);

		ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
		ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory.createDataStore(
				new File("E:\\Data\\OSM\\Japan\\japan-161213-free.shp\\gis.osm_pois_free_1.shp").toURI().toURL());
		sds.setCharset(Charset.forName("GBK"));
		SimpleFeatureSource featureSource = sds.getFeatureSource();
		SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
		Date cdate = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		store.createSchema("japan", featureType.getName().toString().replace(".", "_") + "_" + formatter.format(cdate),
				new Date(), CassandraDataType.VECTOR, new HashMap<>(), featureType);
	}

	public void testFeatureSource() throws Exception {
		Map<String, Serializable> params = new HashMap<>();

		params.put(CassandraDataStoreFactory.DBTYPE.key, "cassandra");
		params.put(CassandraDataStoreFactory.HOST.key, "192.168.210.110");
		params.put(CassandraDataStoreFactory.USER.key, "cassandra");
		params.put(CassandraDataStoreFactory.PASSWD.key, "cassandra");

		Iterator<DataStoreFactorySpi> iterators = DataStoreFinder.getAllDataStores();
		while (iterators.hasNext()) {
			DataStoreFactorySpi spi = iterators.next();
			if (spi.canProcess(params)) {
				System.out.println(spi);
			}
		}
		CassandraDataStoreFactory spi = new CassandraDataStoreFactory();
		CassandraDataStore store = (CassandraDataStore) spi.createDataStore(params);
		SimpleFeatureSource featureSource = store.getFeatureSource(store.getTypeNames()[0]);
		SimpleFeatureType sft = featureSource.getSchema();
		System.out.println(sft);
	}

	public void testReader() throws Exception {

		double lat0 = 34.252;double lon0 = 138.217;
		double lat1 = 36.958;double lon1 = 140.985;

		Filter spatial = CQL.toFilter("BBOX(the_geom," + lon0 + "," + lat0 + "," + lon1 + "," + lat1 + ")");

		Map<String, Serializable> params = new HashMap<>();
		CassandraDataStoreFactory spi = new CassandraDataStoreFactory();
		params.put(CassandraDataStoreFactory.DBTYPE.key, "cassandra");
		params.put(CassandraDataStoreFactory.HOST.key, "192.168.210.110");
		params.put(CassandraDataStoreFactory.USER.key, "cassandra");
		params.put(CassandraDataStoreFactory.PASSWD.key, "cassandra");
		CassandraDataStore datastore = (CassandraDataStore) spi.createDataStore(params);
		SimpleFeatureSource cfeatureSource = datastore.getFeatureSource(datastore.getTypeNames()[0]);

		Query query = new Query(cfeatureSource.getSchema().getTypeName(), spatial, new String[] {});
		System.out.println(cfeatureSource);

		long t0 = System.currentTimeMillis();
		SimpleFeatureCollection features = cfeatureSource.getFeatures(query);
		SimpleFeatureIterator iterator = features.features();
		System.out.println(System.currentTimeMillis() - t0 + " ms");
		System.out.println("Begin to interate...");
		try {
			while (iterator.hasNext()) {
				SimpleFeature feature = iterator.next();
				// System.out.println(feature.getDefaultGeometry());
				Geometry geometry = (Geometry) feature.getDefaultGeometry();
				System.out.println(feature.getID() + " default geometry " +geometry);
			}
		} catch (Throwable t) {
			iterator.close();
		}
		System.out.println("Finished...");
	}

	public void testWriter() throws Exception {

		// Read shapefile
		ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
		ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory.createDataStore(
				new File("E:\\Data\\OSM\\Japan\\japan-161213-free.shp\\gis.osm_pois_free_1.shp").toURI().toURL());
		sds.setCharset(Charset.forName("GBK"));
		SimpleFeatureSource featureSource = sds.getFeatureSource();
		SimpleFeatureType featureType = featureSource.getFeatures().getSchema();

		SimpleFeatureCollection featureCollection = featureSource.getFeatures();

		// Connect Casssandra Datastore
		Map<String, Serializable> params = new HashMap<>();
		CassandraDataStoreFactory spi = new CassandraDataStoreFactory();
		params.put(CassandraDataStoreFactory.DBTYPE.key, "cassandra");
		params.put(CassandraDataStoreFactory.HOST.key, "192.168.210.110");
		params.put(CassandraDataStoreFactory.USER.key, "cassandra");
		params.put(CassandraDataStoreFactory.PASSWD.key, "cassandra");
		System.out.println("Get DataStore...");
		CassandraDataStore datastore = (CassandraDataStore) spi.createDataStore(params);
		Date cdate = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		System.out.println("createSchema...");
		datastore.createSchema("japan",
				featureType.getName().toString().replace(".", "_") + "_" + formatter.format(cdate), new Date(),
				CassandraDataType.VECTOR, new HashMap<>(), featureType);

		System.out.println("Get SimpleFeatureSource: " + datastore.getTypeNames()[0]);
		SimpleFeatureSource cfeatureSource = datastore.getFeatureSource(datastore.getTypeNames()[0]);
		CassandraFeatureStore cfeatureStore = (CassandraFeatureStore) cfeatureSource;
		System.out.println("Begin to import data..");
		cfeatureStore.addFeatures(featureCollection);
		System.out.println("Finished!");
	}

}

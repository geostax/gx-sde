package com.geostax.cassandra;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.CircularString;
import org.geotools.geometry.jts.CompoundCurve;
import org.geotools.geometry.jts.CurvePolygon;
import org.geotools.geometry.jts.MultiCurve;
import org.geotools.geometry.jts.MultiSurface;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.geostax.cassandra.mapper.CassandraObjectMapper;
import com.geostax.cassandra.mapper.Layer;
import com.geostax.sde.data.ContentDataStore;
import com.geostax.sde.data.ContentEntry;
import com.geostax.sde.data.ContentFeatureSource;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class CassandraDataStore extends ContentDataStore {// geometry type to
															// class map
	final static Map<String, Class> TYPE_TO_CLASS_MAP = new HashMap<String, Class>() {
		{
			put("GEOMETRY", Geometry.class);
			put("GEOGRAPHY", Geometry.class);
			put("POINT", Point.class);
			put("POINTM", Point.class);
			put("LINESTRING", LineString.class);
			put("LINESTRINGM", LineString.class);
			put("POLYGON", Polygon.class);
			put("POLYGONM", Polygon.class);
			put("MULTIPOINT", MultiPoint.class);
			put("MULTIPOINTM", MultiPoint.class);
			put("MULTILINESTRING", MultiLineString.class);
			put("MULTILINESTRINGM", MultiLineString.class);
			put("MULTIPOLYGON", MultiPolygon.class);
			put("MULTIPOLYGONM", MultiPolygon.class);
			put("GEOMETRYCOLLECTION", GeometryCollection.class);
			put("GEOMETRYCOLLECTIONM", GeometryCollection.class);
			put("COMPOUNDCURVE", CompoundCurve.class);
			put("MULTICURVE", MultiCurve.class);
			put("CURVEPOLYGON", CurvePolygon.class);
			put("CIRCULARSTRING", CircularString.class);
			put("MULTISURFACE", MultiSurface.class);
			put("BYTEA", byte[].class);
		}
	};

	public static Map<Class, DataType> TYPE_TO_CA_MAP = new HashMap<Class, DataType>() {
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

	public static Map<DataType, Class> CA_MAP_TO_TYPE = new HashMap<DataType, Class>() {
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

	/**
	 * Boolean marker stating whether the feature type is to be considered read
	 * only
	 */
	public static final String CASSANDRA_READ_ONLY = "cassandra.readOnly";
	public SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
	Map<String, Serializable> params;

	public CassandraDataStore(String host) {

		CassandraConnector.init(host);

	}

	//
	// API Implementation
	//

	@Override
	protected List<Name> createTypeNames() throws IOException {
		Session session = CassandraConnector.getSession();
		List<Name> typeNames = new ArrayList<>();
		SimpleStatement statement = new SimpleStatement("SELECT ks,layer_type,layer_name,cdate FROM sde_meta.layer;");
		ResultSet rs = session.execute(statement);
		for (Row row : rs) {
			String ks = row.getString("ks");
			String layer_type = row.getString("layer_type");
			String layer_name = row.getString("layer_name");
			Date cdate = row.getTimestamp("cdate");
			typeNames.add(new NameImpl(ks, layer_name));
		}
		session.close();
		return typeNames;
	}

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
		SimpleFeatureType schema = entry.getState(Transaction.AUTO_COMMIT).getFeatureType();
		if (schema == null) {
			// if the schema still haven't been computed, force its computation
			// so that we can decide if the feature type is read only
			schema = getSchema(entry.getName());
			entry.getState(Transaction.AUTO_COMMIT).setFeatureType(schema);
		}
		schema = entry.getState(Transaction.AUTO_COMMIT).getFeatureType();

		Object readOnlyMarker = schema.getUserData().get(CASSANDRA_READ_ONLY);
		if (Boolean.TRUE.equals(readOnlyMarker)) {
			return new CassandraFeatureSource(entry, Query.ALL);
		} else {
			return new CassandraFeatureStore(entry);
		}
	}

	public void createSchema(String ks, String layername, Date cdate, CassandraDataType ctype,
			Map<String, Double> evenlope, SimpleFeatureType featureType) throws IOException {

		String geometry_type = featureType.getGeometryDescriptor().getType().getName().getLocalPart();
		String geometry_column = "the_geom";

		int srid = getSRID(featureType);

		Layer layer = new Layer();
		layer.setId(UUID.randomUUID());
		layer.setKs(ks);
		layer.setLayer_type(ctype.toString());
		layer.setLayer_name(layername);
		layer.setCdate(cdate);
		layer.setGeometry_column(geometry_column);
		layer.setGeometry_type(geometry_type);
		layer.setSrid(srid);
		layer.setEvenlope(evenlope);

		CassandraObjectMapper.addLayer(layer);

		Session session = CassandraConnector.getSession();
		StringBuilder builder = new StringBuilder();

		builder.append("CREATE TABLE IF NOT EXISTS " + ks + "." + layername + " (");
		builder.append("block text,");
		builder.append("cell text,");
		builder.append("fid text,");
		List<AttributeDescriptor> attrDes = featureType.getAttributeDescriptors();
		List<String> col_items = new ArrayList<>();
		String cols = "";
		for (AttributeDescriptor attr : attrDes) {
			String col_name = attr.getLocalName();
			cols += col_name + ",";
			Class type = attr.getType().getBinding();
			builder.append(col_name + " " + TYPE_TO_CA_MAP.get(type).getName().toString() + ",");
		}
		builder.append("PRIMARY KEY (block,cell,fid)");
		builder.append(");");
		System.out.println(builder.toString());
		session.execute(builder.toString());

		//builder = new StringBuilder();
		//System.out.println(builder.toString());
		//session.execute(builder.toString());
		session.close();
	}

	@Override
	public SimpleFeatureType getSchema(Name name) throws IOException {
		Session session = CassandraConnector.getSession();
		String workspace_name = name.getNamespaceURI();
		String layername = name.getLocalPart();
		KeyspaceMetadata keyspaceMetadata = CassandraConnector.getMetadata().getKeyspace(workspace_name);
		TableMetadata table = keyspaceMetadata.getTable(layername);
		session.close();
		List<ColumnMetadata> columns = table.getColumns();
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(name);
		AttributeTypeBuilder attrTypeBuilder = new AttributeTypeBuilder();
		for (ColumnMetadata cm : columns) {
			String cname = cm.getName();
			Class binding = CA_MAP_TO_TYPE.get(cm.getType());
			if (!cm.getName().equals("cell_id") && !cm.getName().equals("epoch") && !cm.getName().equals("pos")
					&& !cm.getName().equals("timestamp") && !cm.getName().equals("fid")) {
				if (Geometry.class.isAssignableFrom(binding)) {
					attrTypeBuilder.binding(binding);
					CoordinateReferenceSystem wsg84 = null;
					try {
						wsg84 = DefaultGeographicCRS.WGS84;
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

	//
	// Internal methods
	//
	/**
	 * Looks up the geometry srs by trying a number of heuristics. Returns -1 if
	 * all attempts at guessing the srid failed.
	 */

	protected int getSRID(SimpleFeatureType featureType) {
		int srid = -1;
		CoordinateReferenceSystem flatCRS = CRS.getHorizontalCRS(featureType.getCoordinateReferenceSystem());
		try {
			Integer candidate = CRS.lookupEpsgCode(flatCRS, false);
			if (candidate != null)
				srid = candidate;
			else
				srid = 4326;
		} catch (Exception e) {
			// ok, we tried...
		}
		return srid;
	}

}

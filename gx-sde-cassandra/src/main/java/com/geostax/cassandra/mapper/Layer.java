package com.geostax.cassandra.mapper;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "sde_meta", name = "layer", readConsistency = "QUORUM", writeConsistency = "QUORUM", caseSensitiveKeyspace = false, caseSensitiveTable = false)
public class Layer {
	@PartitionKey
	private UUID id;

	private String ks;
	private String layer_name;
	private String layer_type;
	private Date cdate;
	private int srid;
	private String geometry_column;
	private String geometry_type;

	private List<String> keywords;

	private Map<String, Double> evenlope;

	public Layer() {
		// TODO Auto-generated constructor stub
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getKs() {
		return ks;
	}

	public void setKs(String ks) {
		this.ks = ks;
	}

	public String getLayer_name() {
		return layer_name;
	}

	public void setLayer_name(String layer_name) {
		this.layer_name = layer_name;
	}

	public String getLayer_type() {
		return layer_type;
	}

	public void setLayer_type(String layer_type) {
		this.layer_type = layer_type;
	}

	public Date getCdate() {
		return cdate;
	}

	public void setCdate(Date cdate) {
		this.cdate = cdate;
	}

	public int getSrid() {
		return srid;
	}

	public void setSrid(int srid) {
		this.srid = srid;
	}

	public String getGeometry_column() {
		return geometry_column;
	}

	public void setGeometry_column(String geometry_column) {
		this.geometry_column = geometry_column;
	}

	public String getGeometry_type() {
		return geometry_type;
	}

	public void setGeometry_type(String geometry_type) {
		this.geometry_type = geometry_type;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public Map<String, Double> getEvenlope() {
		return evenlope;
	}

	public void setEvenlope(Map<String, Double> evenlope) {
		this.evenlope = evenlope;
	}

}
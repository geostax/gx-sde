package com.geostax.cassandra.stationary.mapper;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "stream", name = "catalog", caseSensitiveKeyspace = false, caseSensitiveTable = false)
public class Catalog {

	@PartitionKey
	private UUID id;
	private String type;
	private String name;
	private String description;
	private String meta;
	private String url;
	private Date latest;
	@Frozen
	private Map<String, Item> items;

	public Catalog() {
		// TODO Auto-generated constructor stub
	}	
	public Catalog(UUID id, String type, String name, String description, String meta, String url,
			Map<String, Item> items) {
		super();
		this.id = id;
		this.type = type;
		this.name = name;
		this.description = description;
		this.meta = meta;
		this.url = url;
		this.items = items;
	}
	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getMeta() {
		return meta;
	}

	public void setMeta(String meta) {
		this.meta = meta;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Map<String, Item> getItems() {
		return items;
	}

	public void setItems(Map<String, Item> items) {
		this.items = items;
	}
	public Date getLatest() {
		return latest;
	}
	public void setLatest(Date latest) {
		this.latest = latest;
	}

}

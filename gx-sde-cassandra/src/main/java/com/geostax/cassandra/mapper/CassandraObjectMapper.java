package com.geostax.cassandra.mapper;

import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.geostax.cassandra.CassandraConnector;

public class CassandraObjectMapper {

	public static void addLayer(Layer layer) {
		Session session = CassandraConnector.getSession();
		MappingManager manager = new MappingManager(session);
		Mapper<Layer> mapper = manager.mapper(Layer.class);
		mapper.save(layer);
		session.close();
	}

	public static Layer getLayer(UUID id) {
		Session session = CassandraConnector.getSession();
		MappingManager manager = new MappingManager(session);
		Mapper<Layer> mapper = manager.mapper(Layer.class);
		return mapper.get(id);

	}
	
	
	public static void main(String[] args) {

	}
}
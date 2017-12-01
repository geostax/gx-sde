package com.geostax.cassandra;

import java.util.PropertyResourceBundle;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class CassandraConnector {
	private static String host = null;
	private static Session instance = null;
	private static Cluster cluster = null;
	private static Lock lock = new ReentrantLock();
	private static PropertyResourceBundle bundle;

	private CassandraConnector() {
	}

	public static void init(String h) {
		host = h;
	}

	public static Session getSession() {
		if (null == cluster) {
			try {
				lock.lock();
				if (null == cluster) {
					Builder builder = Cluster.builder();
					builder = builder.addContactPoint(host);
					cluster = builder.build();
					
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				lock.unlock();
			}
		}
		instance = cluster.connect();
		return instance;
	}

	public static Metadata getMetadata() {
		return cluster.getMetadata();
	}

	public static void close() {
		if (null == cluster) {
			try {
				lock.lock();

				if (null == cluster) {
					cluster.close();
				}
			} finally {
				lock.unlock();
			}
		}
	}
}

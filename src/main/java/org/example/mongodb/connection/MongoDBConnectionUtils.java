package org.example.mongodb.connection;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * This class provides mongodb connection utilities
 * @author koustubh.mokashi
 *
 */
public class MongoDBConnectionUtils {

	private static MongoClient client;
	private static MongoDatabase db;

	static {
		client = new MongoClient("localhost", 27017);
		db = client.getDatabase("demo");
	}
	
	public final static MongoDatabase getMongoDatabase() {
		return db;
	}

}

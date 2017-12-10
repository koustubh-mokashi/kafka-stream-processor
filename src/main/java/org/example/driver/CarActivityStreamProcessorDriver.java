package org.example.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;
import org.example.model.CarActivity;
import org.example.mongodb.connection.MongoDBConnectionUtils;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

/**
 * This class proccess car activity data stream ingested into kafka topic "caractivitiy"
 * @author koustubh.mokashi
 *
 */
public class CarActivityStreamProcessorDriver {

	private static ObjectMapper objectMapper = new ObjectMapper();
	
	@SuppressWarnings("serial")
	public static void main(String args[]) throws InterruptedException {

		/*  Here "config" Represents spark configuration . Here i am running spark application on local cluster
		   name given to this application is CarActivityStreamProcessor	
		*/
		SparkConf config = new SparkConf().setMaster("local[3]").setAppName("CarActivityStreamProcessor");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(config, Durations.milliseconds(500));
		
		/*
		 * kafkaParams represents map containing kafka configurations
		 * here kafka is running locally on port 9092 so "bootstrap.servers" key have 
		 * value "localhost:9092"
		 * */
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");

		/*
		 * Topics is set that contains kafka topics names where 
		 * we are going to publish car activity events
		 */
		Set<String> topics = new HashSet<String>();
		topics.add("caractivity");
		
		/*
		 * Here we are actually representing stream from kafka by passing
		 * all kafka related and other necessary configurations
		 */
		final JavaPairInputDStream<String, String> messageStream = KafkaUtils
				.createDirectStream(javaStreamingContext, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

		/*
		 * Here we are going to intialize connection to mongodb as we 
		 * are going to persist all car activity status events to mongo
		 */
		MongoDatabase mongoDatabase = MongoDBConnectionUtils.getMongoDatabase();
		// carActivityCollection is mongodb collection to hold car activity status events
		MongoCollection<Document> carActivityCollection = mongoDatabase.getCollection("caractivity");
		// carCollection is mongodb collection that represent current status of each of car
		MongoCollection<Document> carCollection = mongoDatabase.getCollection("car");
		
		/*
		 * Here v1._2 is json string that represent car activity event which was published
		 * into kafka by producer through kafka client 
		 */
		JavaDStream<String> lines = messageStream.map(new Function<Tuple2<String,String>, String>() {
			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				return v1._2;
			}
		});
		
		/*
		 * Here we are actually persisting car activity events from each RDD into "caractivity" mongo collection
		 *  and processing upserts operation on "car" mongo collection to update current status of car
		 */
		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			
			/*
			 * This method will get call for each rdd
			 * Over here we used mongodb bulk processor to update and insert records in batch
			 * @see org.apache.spark.api.java.function.VoidFunction#call(java.lang.Object)
			 */
			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				List<WriteModel<Document>> carActivityUpdates = new ArrayList<WriteModel<Document>>();
				List<WriteModel<Document>> carUpdates = new ArrayList<WriteModel<Document>>();
				List<String> recordList = rdd.collect();

				if(!recordList.isEmpty()) {
					recordList.forEach((record)-> {
						Document document = processCarActivityLog(carActivityUpdates , record);
						processCarBulkUpserts(carUpdates, document);
					});
				}
				
				if(carActivityUpdates.size() > 0) {
					carActivityCollection.bulkWrite(carActivityUpdates);
					carCollection.bulkWrite(carUpdates);
				}
			}

			private void processCarBulkUpserts(List<WriteModel<Document>> carUpdates, Document document) {
				Document criteria = new Document();
				criteria.put("_id", document.get("carId"));
				UpdateOptions updateOptions = new UpdateOptions();
				updateOptions.upsert(true);
				Document carDetailsToUpsert = new Document();
				carDetailsToUpsert.put("action", document.get("action"));
				carUpdates.add(new UpdateOneModel<Document>(criteria, new Document("$set", carDetailsToUpsert), updateOptions));
			}

			private Document processCarActivityLog(List<WriteModel<Document>> carActivityUpdates, String record) {
				Document document = new Document();
				populateDocumentFromJSON(document, record);
				carActivityUpdates.add(new InsertOneModel<Document>(document));
				document.put("timestamp", System.currentTimeMillis());
				return document;
			}
			
		});
		
		/*
		 * Here we are starting streaming context
		 * here it starts consuming and processing data stream from kafka
		 */
		javaStreamingContext.start();
		/*
		 * Waiting until terminated
		 */
		javaStreamingContext.awaitTermination();
		
	}
	
	/**
	 * Method to popoulate document with details from json
	 * @param document
	 *        represents document instance to populate
	 * @param value
	 * 		  contains useractivity details in json
	 */
	private static Document populateDocumentFromJSON(Document document, String value) {
		try {
			CarActivity carActivity = objectMapper.readValue(value, CarActivity.class);
			document.put("carId", carActivity.getCarId());
			document.put("action", carActivity.getAction());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}

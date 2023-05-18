package com.learn.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerCallback {
	private static final Logger logger=LoggerFactory.getLogger(Process.class);
	public static void main(String[] args) {
		
		logger.info("Hello");
		System.out.println("hello");
		
		
		Properties properties=new Properties();
		properties.setProperty("bootstrap.servers", "192.168.55.11:9092");
//		properties.setProperty("bootstrap.servers", "PLAINTEXT://192.168.55.11:9092");
//		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
		properties.setProperty("sasl.machanism", "PLAIN");
//		proerties.setProperty("bootstrap.server", "192.168.55.11:9092");
//		
		
//		set producet properties
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
//		set Batch size not recommended on production
		properties.setProperty("batch.size", "400");
		
		
//		create the proucer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		

		
		for(int i=0;i<10;i++) {
			for(int j=0;j<30;j++) {
//				create a producer record
				ProducerRecord<String, String> record= new ProducerRecord<>("first_topic", "This is my "+i+" message");
				
//				Send recored to topic
				producer.send(record,new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e==null) {
							logger.info("Received new metatdata \n"
									+"Pattition : "+metadata.partition()+"\n"
									+"Offset :" + metadata.offset()+"\n"
									+"Timestamp: "+metadata.timestamp());
						}else {
							logger.error("Error while producing", e);
						}
						
					}
				});			
			}

			try {
				Thread.sleep(600);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			}

		
//		Tell the producet to send all the date and flush 
		producer.flush();
		
//		Close the producer
		producer.close();
		
		
	}
}

package com.learn.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerKeys {
	private static final Logger logger=LoggerFactory.getLogger(Process.class);
	public static void main(String[] args) {
		
		logger.info("Hello");
		System.out.println("hello");
		
		
		Properties properties=new Properties();
		properties.setProperty("bootstrap.servers", "192.168.55.11:9092");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
		properties.setProperty("sasl.machanism", "PLAIN");
//		
		
//		set producet properties
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
//		create the proucer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		

		for(int j=0;j<2;j++) {
			
		for(int i=0;i<10;i++) {
			
			String topic="first_topic";
			String key="id_"+i;
			String value="Hello world "+i;
//				create a producer record
				ProducerRecord<String, String> record= new ProducerRecord<>(topic, key,value);
				
//				Send recored to topic
				producer.send(record,new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e==null) {
							logger.info("Key: "+key+" | Pattition : "+metadata.partition());
						}else {
							logger.error("Error while producing", e);
						}
					}
				});			
			}
		
		try {
			Thread.sleep(800);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
		
//		Tell the producet to send all the date and flush 
		producer.flush();
		
//		Close the producer
		producer.close();
		
		
	}
}

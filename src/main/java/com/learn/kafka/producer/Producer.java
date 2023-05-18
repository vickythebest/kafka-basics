package com.learn.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Producer {
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
		
		
//		create the proucer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		
//		create a producer record
		ProducerRecord<String, String> record= new ProducerRecord<>("first_topic", "this is my first message");
		
//		Send recored to topic
		producer.send(record);
		
//		Tell the producet to send all the date and flush 
		producer.flush();
		
//		Close the producer
		producer.close();
		
		
	}
}

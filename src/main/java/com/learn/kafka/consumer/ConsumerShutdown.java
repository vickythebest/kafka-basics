package com.learn.kafka.consumer;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerShutdown {

	private static final Logger log=LoggerFactory.getLogger(ConsumerShutdown.class);
	public static void main(String[] args) {
		log.info("Consumer");
		
		String groupId="my-java-application";
		String topic="first_topic";
		
		Properties properties=new Properties();
		properties.setProperty("bootstrap.servers", "192.168.55.11:9092");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
		properties.setProperty("sasl.machanism", "PLAIN");
		
//		Set producer properties
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		properties.setProperty("group.id", groupId);
		properties.setProperty("auto.offset.reset", "earliest");
		
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
//		get a reference to the main thread
		final Thread mainThread=Thread.currentThread();
		
//		Adding the shutdonw hook
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run() {
				log.info("Detect a shutdonw, lets exit by calling consumer.wakeup()...");
				consumer.wakeup();
				
//				join the main thread to allow the execution of the code in the main thread
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		consumer.subscribe(Arrays.asList(topic));
		
		try {
			
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			
			for(ConsumerRecord<String, String> record:records) {
				log.info("Key: "+record.key()+" Value: "+record.value());
				log.info("partition: "+record.partition()+" Offset: "+record.offset());
			}
		}
	}catch(WakeupException e) {
		log.info("Consumer is starting to shoutdown");
	}catch(Exception e) {
		log.error("Unexpected exception in the consumer"+e);
	}finally {
		consumer.close();
		log.info("consuemr is finally shoutdown");
	}
	}

}

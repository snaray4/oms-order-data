package com.yantriks.statistics.producer;

import java.util.Properties;

import org.apache.commons.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.yantriks.statistics.properties.KafkaProperties;

public class KafkaProd {
	
	//To add kafka properties
	
	
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	
	  public KafkaProd(String topic) {
	  System.out.println("Producer constructor"); 
	  Properties props = new Properties(); 
	  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	  //props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
	  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
	  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName()); 
	  producer = new KafkaProducer<>(props);
	  this.topic = topic;
	  
	  }
	  
		//Send message to kafka topic
		
	public void sendMsgToKafka(JSONObject jsson, KafkaProd kafka) {

			RecordMetadata metadata;
			try {
				metadata = kafka.producer.send(new ProducerRecord<>(kafka.topic, 1, jsson.toString())).get();
				System.out.println("SynchronousProducer Completed with success.");
			} catch (Exception e) {
				System.out.println("Exception while sending message::" + e.getMessage()); 
				e.printStackTrace();
			}
		}
}

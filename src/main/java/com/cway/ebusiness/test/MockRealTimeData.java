package com.cway.ebusiness.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * @author cway
 */
public class MockRealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};  
	private static final Map<String, String[]> provinceCityMap = new HashMap<>();
	
	private KafkaProducer<Integer, String> producer;

	public MockRealTimeData() {
		provinceCityMap.put("Jiangsu", new String[] {"Nanjing", "Suzhou"});
		provinceCityMap.put("Hubei", new String[] {"Wuhan", "Jingzhou"});
		provinceCityMap.put("Hunan", new String[] {"Changsha", "Xiangtan"});
		provinceCityMap.put("Henan", new String[] {"Zhengzhou", "Luoyang"});
		provinceCityMap.put("Hebei", new String[] {"Shijiazhuang", "Tangshan"});  
		producer = new KafkaProducer<>(createProducerConfig());
	}
	
	private Properties createProducerConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.201.101:9092,192.168.201.102:9092,192.168.201.103:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("metadata.broker.list", "192.168.1.105:9092,192.168.1.106:9092,192.168.1.107:9092");
		return props;
	}

	@Override
	public void run() {
		while(true) {	
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			String log = System.currentTimeMillis() + "_" + province + "_" + city + "_"
					+ random.nextInt(1000) + "_" + random.nextInt(10);
			producer.send(new ProducerRecord<>("AdRealTimeLog", log));
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData producer = new MockRealTimeData();
		producer.start();
	}
	
}

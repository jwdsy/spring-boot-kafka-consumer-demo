package cn.miao.kafka;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer<K, T> {

	private final Producer<K, T> producer;
	
	public final static String TOPIC = "person1";
	public final static String TOPIC2 = "person2";
	public final static String TOPIC3 = "kafkaMessageServiceTopic2";
	public final static String TOPIC4 = "kafkaMessageServiceTopic4";

	private KafkaProducer() {
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.21.127:9093");// 此处配置的是kafka的端口
		props.put("serializer.class", "cn.miao.kafka.EntityEncoder");// 配置value的序列化类
		props.put("key.serializer.class", "cn.miao.kafka.EntityEncoder");// 配置key的序列化类
		props.put("request.required.acks", "1");
		producer = new Producer<K, T>(new ProducerConfig(props));
	}

	void produce(K k, T t) {
			producer.send(new KeyedMessage<K, T>(TOPIC, k, t));
			producer.send(new KeyedMessage<K, T>(TOPIC2, k, t));
			producer.send(new KeyedMessage<K, T>(TOPIC3, k, t));
			producer.send(new KeyedMessage<K, T>(TOPIC4, k, t));
	}

	public static void main(String[] args) {
		String key = "key";
		Person p = new Person();
		p.setName("name");
		p.setAge(10);
		p.setPhone("13331189071");
		p.setSex("男");
		p.setBirthday(new Date());
		for (int i = 0; i < 5; i++) {
			new KafkaProducer<String, Person>().produce(key+i, p);
		}
	}
}
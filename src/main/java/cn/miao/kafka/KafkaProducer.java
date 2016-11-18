package cn.miao.kafka;

import java.util.Date;
import java.util.Properties;

import cn.miao.handler.TopicHandler;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer<K, T> {

	private final Producer<K, T> producer;

	private KafkaProducer() {
		Properties props = new Properties();
//		props.put("metadata.broker.list", "192.168.21.127:9093");// test此处配置的是kafka的端口
		props.put("metadata.broker.list", "localhost:9092");// local此处配置的是kafka的端口
		props.put("serializer.class", "cn.miao.kafka.EntityEncoder");// 配置value的序列化类
		props.put("key.serializer.class", "cn.miao.kafka.EntityEncoder");// 配置key的序列化类
		props.put("request.required.acks", "1");
		producer = new Producer<K, T>(new ProducerConfig(props));
	}

	void produce(K k, T t) {
		producer.send(new KeyedMessage<K, T>("topic3", k, t));
	}
	
	void produce(String topic ,K k, T t) {
		producer.send(new KeyedMessage<K, T>(topic, k, t));
	}

	public static void main(String[] args) {
		Integer partitionNum = 8;//该配置是让消息均匀的发送到kafka的各个partitions上（消息存放的partitions在kafka server.properties配置文件中的num.partitions）
		int messageNum = 16;//本次发送消息个数
		String[] topics = {"zs2","ls2"};//消息发送到的topic列表
		Person message = new Person();
		message.setName("name");
		message.setAge(10);
		message.setPhone("13331189071");
		message.setSex("男");
		message.setBirthday(new Date());
		for (int i = 0; i < messageNum; i++) {
			String key = new Integer(i%partitionNum).toString();//参数key就是消息发送到kafka的partition编号
			for(String topic : topics){
				new KafkaProducer<String, Person>().produce(topic, key, message);
			}
		}
	}
	
}



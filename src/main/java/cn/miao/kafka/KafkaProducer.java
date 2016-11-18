package cn.miao.kafka;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer<K, T> {

	private final Producer<K, T> producer;

	private KafkaProducer() {
		Properties props = new Properties();
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
		Integer partitionNum = 4;//该配置是让消息均匀的发送到kafka的各个partitions上（消息存放的partitions在kafka server.properties配置文件中的num.partitions）
		int messageNum = 8;//本次发送消息个数
		for (int i = 0; i < messageNum; i++) {
			String key = new Integer(i%partitionNum).toString();//参数key就是消息发送到kafka的partition编号
			Person person = new Person();
			person.setName("person"+i);
			person.setAge(10+i);
			person.setSex("男");
			new KafkaProducer<String, Person>().produce("person", key, person);
			Student student = new Student();
			student.setName("student"+i);
			student.setAge(20+i);
			student.setSex("女");
			new KafkaProducer<String, Student>().produce("student", key, student);
		}
	}
	
}



package cn.miao;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import cn.miao.kafka.KafkaConsumer;
import cn.miao.kafka.KafkaProducer;
import kafka.javaapi.consumer.ConsumerConnector;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		
		ConfigurableApplicationContext context = new SpringApplicationBuilder(Application.class).web(false).run(args);
		context.start();
		KafkaConsumer kafkaConsumer = context.getBean("kafkaConsumer", KafkaConsumer.class);
		ConsumerConnector consumer = kafkaConsumer.createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(KafkaProducer.TOPIC, new Integer(1));// 一次从主题中获取一个数据  
		topicCountMap.put(KafkaProducer.TOPIC2, new Integer(1));
		topicCountMap.put(KafkaProducer.TOPIC3, new Integer(1));
		topicCountMap.put(KafkaProducer.TOPIC4, new Integer(1));
		kafkaConsumer.consumeList3(consumer, topicCountMap);
		
	}

}
package cn.miao;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import cn.miao.kafka.KafkaConsumer;

@SpringBootApplication
public class Application {
	
	public static void main(String[] args) {
		
		ConfigurableApplicationContext context = new SpringApplicationBuilder(Application.class).web(false).run(args);
		context.start();
		KafkaConsumer consumer = context.getBean("kafkaConsumer", KafkaConsumer.class);
		consumer.consume();
		
	}

}
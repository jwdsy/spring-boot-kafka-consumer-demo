package cn.miao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import cn.miao.kafka.KafkaConsumer;

@SpringBootApplication
public class Application {
	
	private final static Logger logger = LoggerFactory.getLogger(Application.class);
	
	public static void main(String[] args) {
		logger.info("spring boot项目启动.......................");
		ConfigurableApplicationContext context = new SpringApplicationBuilder(Application.class).web(false).run(args);
		context.start();
		KafkaConsumer consumer = context.getBean("kafkaConsumer", KafkaConsumer.class);
		consumer.consume1();
		
	}

}
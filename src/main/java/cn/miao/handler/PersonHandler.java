package cn.miao.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import cn.miao.kafka.Person;

@Component
public class PersonHandler extends MessageHandler<String, Person>{
	
	private static final Logger log = LoggerFactory.getLogger(PersonHandler.class);
	
	public static String topic;
	
	@Value("${kafka.topic.person}")
	public void setTopic(String topic) {
		PersonHandler.topic = topic;
	}

	@Override
	protected void register() {
		MessageFactory.regMessageHandler(topic, this);
	}


}

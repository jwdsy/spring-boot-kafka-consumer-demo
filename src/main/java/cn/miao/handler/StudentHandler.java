package cn.miao.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import cn.miao.kafka.Student;

@Component
public class StudentHandler extends MessageHandler<String, Student>{
	
	private static final Logger log = LoggerFactory.getLogger(StudentHandler.class);
	
	public static String topic;
	
	@Value("${kafka.topic.student}")
	public void setTopic(String topic) {
		StudentHandler.topic = topic;
	}

	@Override
	protected void register() {
		MessageFactory.regMessageHandler(topic, this);
	}


}

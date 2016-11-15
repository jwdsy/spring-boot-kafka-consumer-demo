package cn.miao.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import cn.miao.kafka.Person;

@Component
public class TopicHandler extends MessageHandler<String, Person>{
	
	private static final Logger log = LoggerFactory.getLogger(TopicHandler.class);

	@Override
	protected void register() {
		MessageFactory.regMessageHandler("person1", this);
	}

	@Override
	public void handlerMessage(String key, Person message, long offset) {
		log.info("offset："+offset+"　key："+key+"　message："+message);
	}


}

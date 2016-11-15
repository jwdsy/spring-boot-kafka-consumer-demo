package cn.miao.handler;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public final class MessageFactory {

	private static final Map<String, MessageHandler> handlers = new HashMap<String, MessageHandler>();

	private static final Logger logger = LoggerFactory.getLogger(MessageFactory.class);

	private MessageFactory() {
	}

	protected static void regMessageHandler(String handlerName, MessageHandler messageHandler) {
		handlers.put(handlerName, messageHandler);
	}

	public static MessageHandler getMessageHandler(String handlerName) {
		if (handlerName == null) {
			logger.error("handlerName is null");
			return null;
		}
		return handlers.get(handlerName);
	}
}

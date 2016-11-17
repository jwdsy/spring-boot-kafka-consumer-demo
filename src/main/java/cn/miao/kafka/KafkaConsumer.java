package cn.miao.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import cn.miao.handler.MessageFactory;
import cn.miao.handler.MessageHandler;
import cn.miao.handler.TopicHandler;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

@Component
public class KafkaConsumer {
	
	private final static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
	
	@Value("${kafka.thread}")
	private Integer kafkaThread;
	
	@Value("${zookeeper.connect}")
	private String zookeeperConnect;
	@Value("${group.id}")
	private String groupId;
	@Value("${zookeeper.session.timeout.ms}")
	private String zookeeperSessionTimeoutMs;
	@Value("${zookeeper.sync.time.ms}")
	private String zookeeperSyncTimeMs;
	@Value("${auto.commit.interval.ms}")
	private String autoCommitIntervalMs;
	@Value("${auto.offset.reset}")
	private String autoOffsetReset;

	public ConsumerConnector createConsumer() {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeperConnect);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", zookeeperSessionTimeoutMs);
		props.put("zookeeper.sync.time.ms", zookeeperSyncTimeMs);
		props.put("auto.commit.interval.ms", autoCommitIntervalMs);
		props.put("auto.offset.reset", autoOffsetReset);
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
		return consumer;
	}

	public <K, T> void consume() {
		ConsumerConnector consumer = this.createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		//每次新增一个Handler都在这里把其topic加入到topicCountMap中
		topicCountMap.put(TopicHandler.topic, kafkaThread);// 一次从主题中获取kafkaThread个数据，用kafkaThread个线程
		EntityDecoder<K> keyDecoder = new EntityDecoder<K>();
		EntityDecoder<T> valueDecoder = new EntityDecoder<T>();
		Map<String, List<KafkaStream<K, T>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		Collection<List<KafkaStream<K,T>>> values = consumerMap.values();
		Iterator<List<KafkaStream<K, T>>> iterator = values.iterator();

		// create list of 4 threads to consume from each of the partitions 
		ExecutorService executor = Executors.newFixedThreadPool(topicCountMap.size());

		while (iterator.hasNext()) {
			final List<KafkaStream<K, T>> streams = iterator.next();
			executor.submit(new Runnable() {
				public void run() {
					for (KafkaStream<K, T> stream : streams) {
						ConsumerIterator<K, T> it = stream.iterator();
						while (it.hasNext()) {
							MessageAndMetadata<K, T> mm = it.next();
							String topic = it.kafka$consumer$ConsumerIterator$$currentTopicInfo().topic();
							K key = mm.key();
							T message = mm.message();
							long offset = it.kafka$consumer$ConsumerIterator$$consumedOffset();

							try {
								@SuppressWarnings("unchecked")
								MessageHandler<K, T> handler = MessageFactory.getMessageHandler(topic);
								if (handler != null) {
									System.err.println(1/0);
									logger.error("topic[{}]相应的处理器是{}，offset：{}，key：{}，message：{}", topic, handler.getClass().getName(), offset, key, message);
									handler.handlerMessage(key, message, offset);
								}else{
									logger.error("未找到topic[{}]相应的处理器！", topic);
								}
							} catch (Exception e) {
								logger.error("消息处理遇到异常了", e);
								continue;
							}
							
						}
					}
				}
			});
		}
	}
	
}
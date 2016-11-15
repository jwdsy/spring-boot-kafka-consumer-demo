package cn.miao.kafka;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Component;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.PartitionTopicInfo;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

@Component
public class KafkaConsumer {

	public ConsumerConnector createConsumer() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.21.114:2182");// zookeeper 配置
		props.put("group.id", "test-group1");// group 代表一个消费组
		props.put("zookeeper.session.timeout.ms", "4000");// zk连接超时
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");//先produce一些数据，然后再用consumer读的话，需要加上一句offset读取设置必须要加，如果要读旧数据，因为初始的offset默认是非法的，然后这个设置的意思 是，当offset非法时，如何修正offset，默认是largest，即最新，所以不加这个配置，你是读不到你之前produce的数据的，而且这个 时候你再加上smallest配置也没用了，因为此时offset是合法的，不会再被修正了，需要手工或用工具改重置offset
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
		return consumer;
	}

	public <K, T> void consumeList3(ConsumerConnector consumer, Map<String, Integer> topicCountMap) {
		EntityDecoder<K> keyDecoder = new EntityDecoder<K>();
		EntityDecoder<T> valueDecoder = new EntityDecoder<T>();
		Map<String, List<KafkaStream<K, T>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		Collection<List<KafkaStream<K,T>>> values = consumerMap.values();
		Iterator<List<KafkaStream<K, T>>> iterator = values.iterator();

		// create list of 4 threads to consume from each of the partitions 
		ExecutorService executor = Executors.newFixedThreadPool(topicCountMap.size());

		// consume the messages in the threads
		while (iterator.hasNext()) {
			final List<KafkaStream<K, T>> streams = iterator.next();
			executor.submit(new Runnable() {
				public void run() {
					for (KafkaStream<K, T> stream : streams) {
						// process message (msgAndMetadata.message())
						ConsumerIterator<K, T> it = stream.iterator();
						while (it.hasNext()) {
							MessageAndMetadata<K, T> mm = it.next();
							long offset = it.kafka$consumer$ConsumerIterator$$consumedOffset();
							//System.err.print("offset："+offset+"　");
							PartitionTopicInfo info = it.kafka$consumer$ConsumerIterator$$currentTopicInfo();
							//System.err.print("topic："+info.topic()+"　");
							//System.err.print("key："+mm.key()+"　");
							//System.err.print("value："+mm.message().toString()+"　");
							System.err.println("Thread.currentThread().getId()："+Thread.currentThread().getId()+"　offset："+offset+"　"+"topic："+info.topic()+"　"+"key："+mm.key()+"　"+"value："+mm.message().toString()+"　");
						}
					}
				}
			});
		}
	}
	
}
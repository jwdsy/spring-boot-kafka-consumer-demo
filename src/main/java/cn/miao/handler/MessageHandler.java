package cn.miao.handler;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description: 消息处理器
 * @ClassName: MessageHandler 
 * @author 张飞
 * @date 2016年11月15日 下午8:16:41
 * @version 1.0
 */
public abstract class MessageHandler<K, T>{
	
	private static final Logger log = LoggerFactory.getLogger(StudentHandler.class);

	/**
	 * 注册MessageHandler到factory中
	 */
	protected abstract void register();

	@PostConstruct
	private void init(){
		register();
	}
	
	/**
	 * @Description 消息处理
	 * @Title handlerMessage
	 * @Author :  张飞
	 * @Date :  2016年11月18日 下午9:07:46
	 * @param key 消息key
	 * @param message 消息内容
	 * @return void
	 */
	public void handlerMessage(K key, T message) {
		log.info("　key："+key+"　message："+message);
	}
	/**
	 * @Description 消息处理
	 * @Title handlerMessage
	 * @Author :  张飞
	 * @Date :  2016年11月15日 下午9:13:14
	 * @param key 消息key
	 * @param message 消息内容
	 * @param offset 消息被处理到的位置
	 * @return void
	 */
	public void handlerMessage(K key, T message, long offset) {
		log.info("offset："+offset+"　key："+key+"　message："+message);
	}
	
	/**
	 * @Description 消息处理
	 * @Title handlerMessage
	 * @Author :  张飞
	 * @Date :  2016年11月18日 下午9:08:57
	 * @param key 消息key
	 * @param message 消息内容
	 * @param offset 消息被处理到的位置
	 * @param partition 消息所在的partition
	 * @return void
	 */
	public void handlerMessage(K key, T message, long offset, int partition) {
		log.info("offset："+offset+"　partition："+partition+"　key："+key+"　message："+message);
	}
	

	
}

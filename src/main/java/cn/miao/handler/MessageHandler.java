package cn.miao.handler;

import javax.annotation.PostConstruct;

import cn.miao.exception.ServiceException;

/**
 * @Description: 消息处理器
 * @ClassName: MessageHandler 
 * @author 张飞
 * @date 2016年11月15日 下午8:16:41
 * @version 1.0
 */
public abstract class MessageHandler<K, T>{

	/**
	 * 注册MessageHandler到factory中
	 */
	protected abstract void register();

	@PostConstruct
	private void init(){
		register();
	}
	
	/**
	 * @Description 处理消息
	 * @Title handlerMessage
	 * @Author :  张飞
	 * @Date :  2016年11月15日 下午9:13:14
	 * @param key 消息key
	 * @param message 消息内容
	 * @param offset 消息被处理到的位置
	 * @return void
	 */
	public void handlerMessage(K key, T message, long offset) {
		System.err.println("offset："+offset+"　key："+key+"　message："+message);
	}
	

	
}

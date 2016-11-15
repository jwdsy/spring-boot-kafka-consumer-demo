package cn.miao.kafka;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import kafka.serializer.Decoder;

public class EntityDecoder<T> implements Decoder<T> {

	@SuppressWarnings("unchecked")
	@Override
	public T fromBytes(byte[] entity) {
		T obj = null;
		try {
			// bytearray to object
			ByteArrayInputStream bi = new ByteArrayInputStream(entity);
			ObjectInputStream oi = new ObjectInputStream(bi);
			obj = (T)oi.readObject();
			bi.close();
			oi.close();
		} catch (Exception e) {
			System.out.println("translation" + e.getMessage());
			e.printStackTrace();
		}
		return obj;
	}

}

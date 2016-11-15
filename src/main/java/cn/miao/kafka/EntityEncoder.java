package cn.miao.kafka;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class EntityEncoder<T> implements Encoder<T> {

	@Override
	public byte[] toBytes(T entity) {
		byte[] bytes = new byte[]{};
		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bo);
			oo.writeObject(entity);
			bytes = bo.toByteArray();
			bo.close();
			oo.close();
		} catch (Exception e) {
			System.out.println("translation" + e.getMessage());
			e.printStackTrace();
		}
		return (bytes);
	}

	public EntityEncoder(VerifiableProperties vp) {
		super();
	}

}

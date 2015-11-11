package kafka.pipeline.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Kafka deals solely with bytes over the network so this class provides
 * helper methods to deal with serialization. To serialize a Key or Value
 * use the methods provided by those classes.
 * @author scott
 *
 */
public class SerializationUtils {

	/**
	 * 
	 * @param object
	 * @return
	 * @throws IOException
	 */
	public static byte[] serialize(Object object) throws IOException{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] bytes;

		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(object);
			bytes = bos.toByteArray();
		} finally {
			if (out != null) {
				out.close();
			}
			bos.close();
		}
		return bytes;
	}

	/**
	 * 
	 * @param bytes
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException{ 
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		Object o;
		try {
			in = new ObjectInputStream(bis);
			o = in.readObject(); 
		} finally {
			if (in != null) {
				in.close();
			}
			bis.close();
		}
		return o;
	}
}

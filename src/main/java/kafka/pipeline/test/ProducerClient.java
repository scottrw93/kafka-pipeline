package kafka.pipeline.test;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import kafka.pipeline.store.Key;
import kafka.pipeline.store.Value;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;



public class ProducerClient {
	public static void main(String[] args) throws ClassNotFoundException, IOException {	
		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");


		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

		for(int i = 30; i < 40; i++){
			String ip = "10.0.26."+i;
			String url = "http://testurl"+i+".com";

			Value val = new Value();
			val.put("ip", ip);
			val.put("url", url);
			val.put("location", "Ireland");
			val.put("time", new Date());
			if(i % 2 == 0) val.put("browser", "Chrome");
			if(i % 2 == 1) val.put("browser", "Safari");

			byte[] key = Key.seriailize(new Key(ip, url));
			byte[] value = Value.seriailize(val);

			producer.send(new ProducerRecord<byte[], byte[]>("websitevisits", key, value), new Callback() {
				public void onCompletion(RecordMetadata arg0, Exception arg1) {
					System.out.println("ack");
				}
			});
		}
		producer.close();
	}
}
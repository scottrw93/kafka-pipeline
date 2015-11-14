package kafka.pipeline.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import kafka.pipeline.store.Value;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.TopicPartition;



public class ConsumeClient {
	public static void main(String[] args) throws ClassNotFoundException, IOException {	
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");

		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(properties);	

		consumer.assign(Arrays.asList( new TopicPartition("output", 0)));
		consumer.seekToBeginning(new TopicPartition("output", 0));


		for(ConsumerRecord<byte[], byte[]> record : consumer.poll(1000L)){
			Value value = Value.deseriailize(record.value());
			System.out.println(value);
		}

		consumer.close();
	}
}
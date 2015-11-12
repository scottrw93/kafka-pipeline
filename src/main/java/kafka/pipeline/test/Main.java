package kafka.pipeline.test;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import kafka.pipeline.PipeLine;
import kafka.pipeline.StepHook;
import kafka.pipeline.store.DataStore;
import kafka.pipeline.store.Key;
import kafka.pipeline.store.Message;
import kafka.pipeline.store.Value;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;



public class Main {
	public static void main(String[] args) throws ClassNotFoundException, IOException {	

		produce();
		produce();

		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");

		Properties producerProps = new Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
		producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
		
		/**
		 * This is simple example that outputs an ip address for every time a user has
		 * visited a site twice on a Chrome browser
		 */
		PipeLine pipe = new PipeLine(consumerProps, producerProps);

		pipe.input(new TopicPartition[]{
				new TopicPartition("websitevisits", 0)
		});

		/**
		 * Got though all messages and and mark all those who have returned to
		 * the site
		 */
		pipe.addStep(new StepHook(1) {
			public void execute(PipeLine pipe) throws ClassNotFoundException, IOException {
				Message msg = pipe.poll("websitevisits");
				DataStore store = pipe.getStore("user-visits");
				if(store.containsKey(msg.key())){
					msg.value().put("returned", 1);
				}
				store.put(msg.key(), msg.value());
			}
		});

		/**
		 * Got through all those who have visited a site at least twice on a chrome browser 
		 * and output them.
		 */
		pipe.addStep(new StepHook(2) {
			public void execute(PipeLine pipe) throws ClassNotFoundException, IOException {
				DataStore store = pipe.getStore("user-visits");
				DataStore prevOutput = pipe.getStore("prev-output");

				for(Key key : store.keySet()){
					Value value = store.get(key);

					if(value.get("returned") != null){
						if(!prevOutput.containsKey(key) &&
								value.get("browser").equals("Chrome")){
							pipe.output(new Message(null, value), "output");
							prevOutput.put(key, null);
						}
					}
				}
			}
		});

		pipe.run();

		for(Message msg : pipe.getOutput()){
			System.out.println(msg.value());
		}
	}

	private static void produce() throws IOException {
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
					//ack
				}
			});
		}
		producer.close();
	}

}
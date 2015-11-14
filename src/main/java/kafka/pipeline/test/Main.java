package kafka.pipeline.test;

import java.io.IOException;
import java.util.Properties;

import kafka.pipeline.PipeLine;
import kafka.pipeline.StepHook;
import kafka.pipeline.store.DataStore;
import kafka.pipeline.store.Key;
import kafka.pipeline.store.Message;
import kafka.pipeline.store.Value;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;



public class Main {
	public static void main(String[] args) throws ClassNotFoundException, IOException {	
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
	}
}
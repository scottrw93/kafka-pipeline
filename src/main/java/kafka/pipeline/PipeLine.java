package kafka.pipeline;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.TreeMap;

import kafka.pipeline.store.DataStore;
import kafka.pipeline.store.Key;
import kafka.pipeline.store.Message;
import kafka.pipeline.store.Value;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;

public class PipeLine {

	private TreeMap<Integer, StepHook> steps;
	private HashMap<String, DataStore> stores;
	private Queue<Message> outputQueue;
	private Map<String, Queue<ConsumerRecord<byte[], byte[]>>> topicInputQueues;

	private KafkaConsumer<byte[], byte[]> consumer;
	private Properties props;
	private Map<TopicPartition, OffsetAndMetadata> lastOffsets;

	/**
	 * Must commit offset synchronously so do it in a batch for performance. Allows
	 * at-least-once semantics.
	 */
	private final int offsetBatchSize = 5;
	private int offsetBatchCounter;

	/**
	 * The pipeline can consume messages from Kafka and allows the use of hooks to
	 * manipulate the consumed messages and to store them in in-memory databases.
	 * The pipeline can also output data to a single topic for use with another
	 * application.
	 * 
	 * This currently consumes messages from the beggining of each topic. I intend to
	 * alter this to consume from the last commited offset of each topic from the previous
	 * run but to do this I need to implement check pointing for the datastores.
	 * 
	 * @param props - configuration for the Kafka Consumer
	 */
	public PipeLine(Properties props){
		this.steps = new TreeMap<Integer, StepHook>();
		this.stores = new HashMap<String, DataStore>();
		this.outputQueue = new LinkedList<Message>();
		this.topicInputQueues = new HashMap<String, Queue<ConsumerRecord<byte[], byte[]>>>();
		this.lastOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
		this.offsetBatchCounter = 0;
		this.props = props;
	}

	/**
	 * This method retrieves the topics that messages are to be consumed from
	 * @param partitions - topics that messages are to be consumed from
	 */
	public void input(TopicPartition... partitions){
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");

		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");

		consumer = new KafkaConsumer<byte[], byte[]>(props);	

		consumer.assign(Arrays.asList(partitions));
		//consumer.seekToBeginning(partitions);

		for(TopicPartition topic : partitions){
			topicInputQueues.put(topic.topic(), new LinkedList<ConsumerRecord<byte[], byte[]>>());
		}
	}

	/**
	 * Consume a message from the given topic. This method is guranteed to return
	 * a Message
	 * @param topic - the topic you wish to poll from
	 * @return - a message from the specified topic
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public Message poll(String topic) throws ClassNotFoundException, IOException{
		ConsumerRecord<byte[], byte[]> record = topicInputQueues.get(topic).poll();

		Key key = Key.deseriailize(record.key());
		Value value = Value.deseriailize(record.value());
		long offset = record.offset();

		lastOffsets.put(new TopicPartition(topic, 0), new OffsetAndMetadata(offset));
		offsetBatchCounter++;

		if(offsetBatchCounter == offsetBatchSize){
			try{
				consumer.commitSync(lastOffsets);
				lastOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
				offsetBatchCounter = 0;
			} catch(WakeupException e){

			} catch(AuthorizationException e){

			} catch(Exception e){
				
			}
		}

		return new Message(key, value);
	}

	/**
	 * Retrieve a datastore associated with the specified id. If none exists with
	 * the given id it will be created.
	 * @param id - id of datastore to retrieve
	 * @return - datastore associated with the given id
	 */
	public DataStore getStore(String id){
		if(!stores.containsKey(id))
			stores.put(id, new DataStore());
		return stores.get(id);
	}

	/**
	 * This method currently stores the messages in a list. The intention is to output
	 * the messages to a topic later.
	 * @param pair
	 */
	public void output(Message pair){
		outputQueue.add(pair);
	}

	/**
	 * Add a hook to the pipeline
	 * @param hook - the code to execute a part of the hook
	 */
	public void addStep(StepHook hook){
		steps.put(hook.id(), hook);
	}

	/**
	 * This method starts the pipeline until all message from the topics have been consumed.
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public void run() throws ClassNotFoundException, IOException{
		while(pollConsumers()){
			for(int i : steps.keySet()){
				steps.get(i).execute(this);
			}
		}
		consumer.commitSync(lastOffsets);
		consumer.close();
	}

	/** 
	 * Tempoary method to see the output from the pipeline
	 * @return
	 */
	public Queue<Message> getOutput(){
		return outputQueue;
	}

	/**
	 * Poll all of the topics specified to retrieve messages. If any topic has no more 
	 * messages to be consumed return false
	 * @return - if any topic has more messages to be consumed
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private boolean pollConsumers() throws ClassNotFoundException, IOException {
		if(checkForMsgs()){
			ConsumerRecords<byte[], byte[]> msgs = consumer.poll(1000L);
			for(ConsumerRecord<byte[], byte[]> record : msgs){
				System.out.println("Adding msg");
				topicInputQueues.get(record.topic()).add(record);
			}
		}
		return !checkForMsgs();
	}

	/**
	 * Check if the input queues are empty.
	 * @return
	 */
	private boolean checkForMsgs() {
		for(String topic : topicInputQueues.keySet()){
			if(topicInputQueues.get(topic).isEmpty()){
				return true;
			}
		}
		return false;
	}
}

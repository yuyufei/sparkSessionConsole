package cn.com.pander.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 模拟生产上向kafka写数据过程
 * @author fly
 *
 */
public class SessionInfoProducer {
	
	public static void main(String[] args) {
		//消息发送模式
		boolean isAsync = args.length == 0
				|| !args[0].trim().equalsIgnoreCase("sync");
		Properties properties=new Properties();
		properties.put("bootstrap.servers","master:9092" );
		properties.put("client.id", "SessionInfoProducer");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		//配置生产者
		KafkaProducer<Integer, String> producer=
				new KafkaProducer<>(properties);
		String topic="testData";
		int message_key=1;
		while(true) 
		{
			 String messageValue = "Message_" + message_key;
	            long startTime = System.currentTimeMillis();
	            if (isAsync) {
	                ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, message_key, messageValue);
	                producer.send(record, new ProducerCallBack<>(startTime, message_key, messageValue));
	            } else {
	                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, message_key, messageValue);
	                try {
	                    RecordMetadata recordMetadata = producer.send(producerRecord).get();
	                    System.out.println("----------------"+recordMetadata.partition());
	                } catch (Exception e) {
	                    throw new RuntimeException(e);
	                }
	            }
	            message_key++;
		}
		
	}

}

class ProducerCallBack<K,V> implements Callback
{
	private final long startTime;
	private final K key;
	private final V value;
	
	

	public ProducerCallBack(long startTime, K key, V value) {
		this.startTime = startTime;
		this.key = key;
		this.value = value;
	}



	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) 
	{
	     if(metadata != null) 
	     {
	    	 long elapsedTime = System.currentTimeMillis() - startTime;
	            System.out.println("message(" + key + "," + value + ") send to partition("
	                    + metadata.partition() + ")," + "offset(" + metadata.offset() + ") in" + elapsedTime);
	     }	
	     else
	     exception.printStackTrace();
	}
}



package cn.com.pander.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SessionInfoConsumer {
	    private String topic;
	    private String path;
	    
	    
	    public SessionInfoConsumer(String topic, String path) {
			this.topic = topic;
			this.path = path;
		}
		public static void main(String[] args) {
			SessionInfoConsumer consumer=new SessionInfoConsumer("testData", "d:\\clusterMonitor.rrd");
			consumer.receive();
	    }   
	    public void receive() {
	        System.out.println("start runing consumer");

	        Properties properties = new Properties();
	        properties.put("zookeeper.connect",
	                "192.168.189.128:2181");
	        properties.put("group.id", "test-consumer-group");
	        ConsumerConnector consumer = Consumer
	                .createJavaConsumerConnector(new ConsumerConfig(properties));

	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
	        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer
	                .createMessageStreams(topicCountMap);
	        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
	        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
	        while (iterator.hasNext()) {
	            String message = new String(iterator.next().message());
	            //hostName+";"+ip+";"+commandName+";"+res+";"+System.currentTimeMillis();
	            //这里指的注意，如果没有下面这个语句的执行很有可能回从头来读消息的
	            consumer.commitOffsets();
	            System.out.println(message);
	        }
	    }

}

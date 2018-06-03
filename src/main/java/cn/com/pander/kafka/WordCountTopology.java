package cn.com.pander.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * 模拟计数统计
 * @author fly
 *
 */
public class WordCountTopology {
	public static void main(String[] args) throws IOException {
		Properties properties=new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
		properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "master:2181");
		properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		TopologyBuilder builder=new TopologyBuilder();
		builder.addSource("SOURCE", new StringDeserializer(),new StringDeserializer(),"testData")
		.addProcessor("WordCountProcessor",WordCountProcessor :: new, "SOURCE")
		.addStateStore(Stores.create("Counts").withStringKeys().withIntegerValues().inMemory().build(), "WordCountProcessor")
		.addSink("SINK", "count", new StringSerializer(),new IntegerSerializer(),"WordCountProcessor");
		
		KafkaStreams streams=new KafkaStreams(builder, properties);
		streams.start();
		System.in.read();
		streams.close();
		streams.cleanUp();
	}

}

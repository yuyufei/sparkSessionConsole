package cn.com.pander.kafka;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class WordCountProcessor implements ProcessorSupplier<String, String>{

	@Override
	public Processor<String, String> get() {
		return new Processor<String, String>() {

			private ProcessorContext context;
			  private KeyValueStore<String, Integer> kvStore;
			  
			  @SuppressWarnings("unchecked")
			  @Override
			  public void init(ProcessorContext context) {
			    this.context = context;
			    this.context.schedule(1000);
			    this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
			  }

			  @Override
			  public void process(String key, String value) {
			    Stream.of(value.toLowerCase().split(" ")).forEach((String word) -> {
			    	System.out.println("word is :"+word);
			      Optional<Integer> counts = Optional.ofNullable(kvStore.get(word));
			      int count = counts.map(wordcount -> wordcount + 1).orElse(1);
			      kvStore.put(word, count);
			    });
			  }

			  @Override
			  public void punctuate(long timestamp) {
			    KeyValueIterator<String, Integer> iterator = this.kvStore.all();
			    iterator.forEachRemaining(entry -> {
			    	System.out.println("key is : "+entry.key+"value is : "+entry.value);
			      context.forward(entry.key, entry.value);
			      this.kvStore.delete(entry.key);
			    });
			    context.commit();
			  }

			  @Override
			  public void close() {
			    this.kvStore.close();
			  }


		};
	}
	
	
}

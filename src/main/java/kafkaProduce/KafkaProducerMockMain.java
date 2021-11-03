package kafkaProduce;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerMockMain {
	
	
	public static void main(String[] args) {
		
		ExecutorService executor = Executors.newFixedThreadPool(4);
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9092,localhost:9094");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FloatSerializer.class.getName());
		
		KafkaProducerMock producer1 = new KafkaProducerMock(props, Sensor.temp);
		KafkaProducerMock producer2 = new KafkaProducerMock(props, Sensor.humidity);
		KafkaProducerMock producer3 = new KafkaProducerMock(props, Sensor.smoke);
		
		while(!Thread.interrupted()) {
			executor.execute(producer1);
			executor.execute(producer2);
			executor.execute(producer3);
		}
		executor.shutdown();
		
	}

}

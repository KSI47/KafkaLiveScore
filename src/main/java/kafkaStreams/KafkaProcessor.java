package kafkaStreams;

import java.math.BigDecimal;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.json.JSONObject;

public class KafkaProcessor implements Processor<String, String> {
	private ProcessorContext context;
	private KafkaProducer<String, String> outProd;

	@Override
	public void init(ProcessorContext context) {
		this.context = context;
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.outProd = new KafkaProducer<String, String>(props);
	}

	@Override
	public void process(String key, String value) {
		if (value.contains("null")) {

		} else {
			processRecord(key, value);

			// forward the processed data to processed-topic topic
			context.forward(key, value);
		}
		context.commit();
	}

	@Override
	public void close() {
	}

	private void processRecord(String key, String value) {
		JSONObject event = new JSONObject(value);

		if ((BigDecimal.valueOf(event.getDouble("temp")).floatValue() > 70)
				&& (BigDecimal.valueOf(event.getDouble("smoke")).floatValue() > 14)) {
			System.out.println("fire");
			outProd.send(new ProducerRecord<String, String>("processed_data", key,
					new JSONObject().put("area", key).put("type", "fire").put("time", event.getString("time")).toString()));

		}
		else {
			System.out.println("no_fire");
			outProd.send(new ProducerRecord<String, String>("processed_data", key,
					new JSONObject().put("area", key).put("type", "no_fire").put("time", event.getString("time")).toString()));
		}

	}

}

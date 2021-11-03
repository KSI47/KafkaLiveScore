package kafkaStreams;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.json.JSONObject;

public class KafkaStreaming {
	private KafkaStreams streamsInnerJoin;
	private final String TEMP_TOPIC = "temp_sensor";
	private final String SMOKE_TOPIC = "smoke_sensor";
	private final String OUTER_JOIN_STREAM_OUT_TOPIC = "temp_smoke_topic";
	private final String PROCESSED_STREAM_OUT_TOPIC = "processed_data";

	private final String KAFKA_APP_ID = "sensors_join";
	private final String KAFKA_SERVER_NAME = "localhost:9092,localhost:9093,localhost:9094";

	public void startStreamStreamInnerJoin() {

		Properties props1 = new Properties();
		props1.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_APP_ID);
		props1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_NAME);
		props1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		KStream<String, Float> tempKStream = builder.stream(TEMP_TOPIC, Consumed.with(Serdes.String(), Serdes.Float()));
		KStream<String, Float> smokeKStream = builder.stream(SMOKE_TOPIC,
				Consumed.with(Serdes.String(), Serdes.Float()));

		KStream<String, String> joined = tempKStream.join(smokeKStream,
				(tempValue, smokeValue) -> new JSONObject().putOpt("temp", tempValue).putOpt("smoke", smokeValue)
						.put("time",
								new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
										.format(new Timestamp(Calendar.getInstance (TimeZone.getTimeZone ("GMT-3:00")). getTimeInMillis ())))
						.toString(),
				JoinWindows.of(Duration.ofSeconds(5)),
				StreamJoined.with(Serdes.String(), Serdes.Float(), Serdes.Float()));

		joined.to(OUTER_JOIN_STREAM_OUT_TOPIC);

		final Topology topology = builder.build();

		topology.addSource("Source", OUTER_JOIN_STREAM_OUT_TOPIC);

		topology.addProcessor("StateProcessor", new ProcessorSupplier<String, String>() {
			public Processor<String, String> get() {
				return new KafkaProcessor();
			}
		}, "Source");


		streamsInnerJoin = new KafkaStreams(topology, props1);

		streamsInnerJoin.start();

	}

	public static void main(String[] args) {

		KafkaStreaming streamer = new KafkaStreaming();

		streamer.startStreamStreamInnerJoin();
	}
}

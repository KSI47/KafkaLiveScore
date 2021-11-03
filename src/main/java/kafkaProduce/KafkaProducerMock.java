package kafkaProduce;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

enum Sensor {
	temp, humidity, smoke
};

public class KafkaProducerMock implements Runnable {

	private KafkaProducer<String, Float> producer;
	private List<String> locations;
	private Sensor sensorType;

	public KafkaProducerMock(Properties props, Sensor sensorType) {
		this.producer = new KafkaProducer<>(props);
		this.sensorType = sensorType;
		this.locations = Arrays.asList("Pi�ce_0", "Pi�ce_1", "Pi�ce_2", "Pi�ce_3", "Pi�ce_4", "Pi�ce_5", "Pi�ce_6",
				"Pi�ce_7", "Pi�ce_8", "Pi�ce_9");
	}

	public void run() {
		Random r = new Random();
		try {
			if (this.sensorType.equals(Sensor.temp)) {
				while (true) {
					for (int i = 0; i < locations.size(); i++) {
						producer.send(new ProducerRecord<>("temp_sensor", locations.get(i), r.nextFloat() * 50 + 30));
						Thread.sleep(100);
					}
				}

			}

			if (this.sensorType.equals(Sensor.humidity)) {
				while (true) {
					for (int i = 0; i < locations.size(); i++) {
						producer.send(new ProducerRecord<>("humid_sensor", locations.get(i), r.nextFloat() * 50 + 50));
						Thread.sleep(100);
					}
				}

			}
			if (this.sensorType.equals(Sensor.smoke)) {
				while (true) {
					for (int i = 0; i < locations.size(); i++) {
						producer.send(new ProducerRecord<>("smoke_sensor", locations.get(i), r.nextFloat() * 5 + 10));
						Thread.sleep(100);
					}
				}
			}

		} catch (InterruptedException e) {
			producer.close();
			System.out.println("### Finishing producer ###");
		}

	}

}

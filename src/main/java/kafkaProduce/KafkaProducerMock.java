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
		this.locations = Arrays.asList("Pièce_0", "Pièce_1", "Pièce_2", "Pièce_3", "Pièce_4", "Pièce_5", "Pièce_6",
				"Pièce_7", "Pièce_8", "Pièce_9");
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

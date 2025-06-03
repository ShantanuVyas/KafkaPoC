package org.example;

import org.apache.kafka.clients.producer.*;

import java.time.Instant;
import java.util.Properties;

public class ProducerApp {
    public static void main(String[] args) {
        String topic = "new-topic";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i <= 10; i++) {
                String key = "key-" + i;
                String value = "Hello Kafka " + i + ":" + Instant.now().toEpochMilli();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)%n", key, value, metadata.partition(), metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
            }
            producer.flush();
            // dummy change
        }
    }
}

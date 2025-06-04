package org.example;

import org.apache.kafka.clients.producer.*;

import java.time.Instant;
import java.util.Properties;

public class ProducerApp {
    public static void main(String[] args) {
        String topic = ConfigLoader.get("topicName");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigLoader.get("bootstrapServers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Load from config
        int messageCount = ConfigLoader.getInt("producer.message.count", 100);
        int sleepMs = ConfigLoader.getInt("producer.sleep.ms", 100);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i <= messageCount; i++) {
                String key = "key-" + i;
                String value = "Hello Kafka " + i + "|" + Instant.now().toEpochMilli();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)%n", key, value, metadata.partition(), metadata.offset());
                    } else {
                        System.err.printf("Error sending record(key=%s value=%s): %s%n", key, value, exception.getMessage());
                        exception.printStackTrace();
                    }
                });
                Thread.sleep(sleepMs);
            }
            producer.flush();
        } catch (Exception e) {
            System.err.println("Error in ProducerApp: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("ProducerApp finished.");
        }
    }
}

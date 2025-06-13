package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ConfigLoader {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream("app.properties")) {
            if (input == null) {
                throw new RuntimeException("app.properties not found in resources directory!");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }

    public static int getInt(String key, int defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public static void createTopicIfNotExists() {
        String topic = get("topicName");
        int partitions = getInt("topicPartitions",1);
        short replicationFactor = (short) getInt("topicReplicationFactor", 1);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, get("bootstrap.servers"));
        // Create topic if it doesn't exist
        try (AdminClient adminClient = AdminClient.create(props)) {
            // Check if topic already exists
            System.out.println("Checking if topic exists: " + topic);
            Set<String> existingTopics = adminClient.listTopics().names().get();

            if (existingTopics.contains(topic)) {
                System.out.println("Topic already exists: " + topic);
            } else {
                // Define topic configuration
                System.out.println("Topic: " + topic + " does not exist, creating it with partitions: " + partitions + " and replication factor: " + replicationFactor);
                NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic created: " + topic);
            }
        } catch (ExecutionException e) {
            System.err.println("Error in topic creation: " + e.getCause().getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
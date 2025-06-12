package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class DynamicConsumerManager {
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final String groupProtocol;
    private final List<IConsumerWorker> workers = new ArrayList<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public DynamicConsumerManager(String bootstrapServers, String topic, String groupId, String groupProtocol) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.groupProtocol = groupProtocol;
    }

    // Add a new consumer worker and start it in a new thread
    public synchronized void addConsumer() {
        IConsumerWorker worker;
        if(groupProtocol.equals("share")){
            worker = new ConsumerSharedWorker(bootstrapServers,topic,groupId);
        }else {
            worker = new ConsumerGroupWorker(bootstrapServers, topic, groupId);
        }

        workers.add(worker);
        executorService.submit(worker);
        System.out.println("Added consumer " + worker.hashCode() + " at "+ Instant.now().toEpochMilli() +". Total consumers: " + workers.size());
    }

    // Remove a consumer worker and stop its thread
    public synchronized void removeConsumer() {
        if (workers.isEmpty()) {
            System.out.println("No consumers to remove.");
            return;
        }
        IConsumerWorker worker = workers.remove(workers.size() - 1);
        System.out.println("Removing consumer " + worker.hashCode());
        worker.shutdown();
        System.out.println("Removed consumer at " + Instant.now().toEpochMilli() + ". Total consumers: " + workers.size());
    }

    // Shutdown all consumers and the executor service
    public synchronized void shutdown() {
        for (IConsumerWorker worker : workers) {
            worker.shutdown();
        }
        executorService.shutdown();
        System.out.println("Manager shutdown.");
    }

    interface IConsumerWorker extends Runnable{
        public void shutdown();
    }
    // Consumer worker class
    static class ConsumerGroupWorker implements IConsumerWorker {
        private  KafkaConsumer<String, String> consumer;
        private final String topic;
        private volatile boolean running = true;
        private final int pollMs;
        private final int sleepMs;

        public ConsumerGroupWorker(String bootstrapServers, String topic, String groupId) {
            this.topic = topic;
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConfigLoader.get("consumer.max.poll.records") != null ? ConfigLoader.get("consumer.max.poll.records") : "1");

            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Collections.singletonList(topic));

            // Load from config
            this.pollMs = ConfigLoader.getInt("consumer.poll.ms", 100);
            this.sleepMs = ConfigLoader.getInt("consumer.sleep.ms", 1000);
        }

        @Override
        public void run() {

            Set<TopicPartition> currentAssignment = new HashSet<>();
            try {
                while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMs));

                    Set<TopicPartition> newAssignment = consumer.assignment();
                    if (!newAssignment.equals(currentAssignment)) {
                        currentAssignment = new HashSet<>(newAssignment);
                        System.out.printf("Consumer %s with Thread %s - Assigned partitions: %s at %d%n",
                                this.hashCode(),Thread.currentThread().getName(), currentAssignment, Instant.now().toEpochMilli());
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        String[] parts = record.value().split("\\|");
                        String val = parts[0];
                        long sentTs = Long.parseLong(parts[1]);
                        long now = Instant.now().toEpochMilli();
                        long elapsed = now - sentTs;

                        System.out.printf(
                                "Consumer %s with partition %s and Thread %s - Consumed record(key=%s value=%s) sent at %d, now %d, elapsed %d ms at offset %d%n",
                                this.hashCode(), currentAssignment, Thread.currentThread().getName(), record.key(), val, sentTs, now, elapsed, record.offset()
                        );

                        if(sleepMs!=0) {
                            Thread.sleep(sleepMs);  //artificial delay to simulate processing time
                        }
                    }
                }
            } catch (WakeupException we){
                //ignore
            } catch (Exception e) {
                // Ignore if shutting down
                System.out.println("Exception in DynamicConsumerManager.ConsumerWorker: " + e.getMessage());
                e.printStackTrace();
            } finally {
                consumer.close();
                System.out.printf("Thread %s - Consumer closed%n", Thread.currentThread().getName());
            }
        }

        public void shutdown() {
            running = false;
            consumer.wakeup();
        }
    }

    static class ConsumerSharedWorker implements IConsumerWorker {
        private  KafkaShareConsumer<String,String> sharedConsumer;
        private final String topic;
        private volatile boolean running = true;
        private final int pollMs;
        private final int sleepMs;

        public ConsumerSharedWorker(String bootstrapServers, String topic, String groupId) {
            this.topic = topic;
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConfigLoader.get("consumer.max.poll.records") != null ? ConfigLoader.get("consumer.max.poll.records") : "1");
            props.put("share.acknowledgement.mode", "explicit");

            this.sharedConsumer = new KafkaShareConsumer<String, String>(props);
            this.sharedConsumer.subscribe(Collections.singletonList(topic));

            // Load from config
            this.pollMs = ConfigLoader.getInt("consumer.poll.ms", 100);
            this.sleepMs = ConfigLoader.getInt("consumer.sleep.ms", 1000);
        }

        @Override
        public void run() {

            Set<TopicPartition> currentAssignment = new HashSet<>();
            try {
                while (running) {
                    ConsumerRecords<String, String> records = sharedConsumer.poll(Duration.ofMillis(pollMs));
                    /*if(!records.isEmpty()){
                        System.out.printf("Consumer %s and Number of records polled %s%n",this.hashCode(),records.count());
                    }*/

                    for (ConsumerRecord<String, String> record : records) {
                        String[] parts = record.value().split("\\|");
                        String val = parts[0];
                        long sentTs = Long.parseLong(parts[1]);
                        long now = Instant.now().toEpochMilli();
                        long elapsed = now - sentTs;

                        System.out.printf(
                                "Consumer %s with partition %s and Thread %s - Consumed record(key=%s value=%s) sent at %d, now %d, elapsed %d ms at offset %d%n",
                                this.hashCode(), currentAssignment, Thread.currentThread().getName(), record.key(), val, sentTs, now, elapsed, record.offset()
                        );

                        if(sleepMs!=0) {
                            Thread.sleep(sleepMs);  //artificial delay to simulate processing time
                        }
                        sharedConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                        sharedConsumer.commitSync();//Can be made async
                    }
                }
            } catch (WakeupException we){
                //ignore
            } catch (Exception e) {
                // Ignore if shutting down
                System.out.println("Exception in DynamicConsumerManager.ConsumerWorker: " + e.getMessage());
                e.printStackTrace();
            } finally {
                if(sharedConsumer!=null){
                    sharedConsumer.close();
                }
                System.out.printf("Thread %s - Consumer closed%n", Thread.currentThread().getName());
            }
        }

        public void shutdown() {
            running = false;
            sharedConsumer.wakeup();
        }
    }
    // For demo: add/remove consumers interactively
    public static void main(String[] args) throws Exception {
        DynamicConsumerManager manager = new DynamicConsumerManager(ConfigLoader.get("bootstrapServers"), ConfigLoader.get("topicName"), ConfigLoader.get("groupId"), ConfigLoader.get("group.protocol"));
        Scanner scanner = new Scanner(System.in);

        // Read initial consumer count from config
        int initialConsumers = ConfigLoader.getInt("consumer.initial.count", 1);
        for (int i = 0; i < initialConsumers; i++) {
            manager.addConsumer();
        }

        System.out.println("Commands: add, remove, exit");
        while (true) {
            System.out.print("> ");
            String command = scanner.nextLine().trim().toLowerCase();
            switch (command) {
                case "add":
                    manager.addConsumer();
                    break;
                case "remove":
                    manager.removeConsumer();
                    break;
                case "exit":
                    manager.shutdown();
                    return;
                default:
                    System.out.println("Unknown command.");
            }
        }
    }
}
package com.lab.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentLatencyTest {
    static class ProducerThread extends Thread {
        private String topic;
        private int messageCount;
        private CountDownLatch producerDone;

        ProducerThread(String topic, int messageCount, CountDownLatch producerDone) {
            this.topic = topic;
            this.messageCount = messageCount;
            this.producerDone = producerDone;
        }

        @Override
        public void run() {
            try {
                Properties properties = new Properties();
                try (InputStream input = getClass().getClassLoader().getResourceAsStream("producer.properties")) {
                    if (input != null) properties.load(input);
                }
                properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                    for (int i = 1; i <= messageCount; i++) {
                        long sendTimeNanos = System.nanoTime();
                        String value = sendTimeNanos + ":Message " + i;
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + i, value);
                        producer.send(record);
                    }
                    producer.flush();
                }
                System.out.println("Producer: All " + messageCount + " messages sent.");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                producerDone.countDown();
            }
        }
    }

    static class ConsumerThread extends Thread {
        private String topic;
        private int expectedMessages;
        private long testDurationMs;
        private List<Long> latencies;
        private CountDownLatch producerDone;

        ConsumerThread(String topic, int expectedMessages, long testDurationMs, 
                      List<Long> latencies, CountDownLatch producerDone) {
            this.topic = topic;
            this.expectedMessages = expectedMessages;
            this.testDurationMs = testDurationMs;
            this.latencies = latencies;
            this.producerDone = producerDone;
        }

        @Override
        public void run() {
            try {
                Properties properties = new Properties();
                try (InputStream input = getClass().getClassLoader().getResourceAsStream("consumer.properties")) {
                    if (input != null) properties.load(input);
                }
                properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
                    consumer.subscribe(Collections.singletonList(topic));
                    System.out.println("Consumer: Subscribed to " + topic);

                    int messagesConsumed = 0;
                    long startTime = System.currentTimeMillis();
                    long endTime = startTime + testDurationMs;

                    while (messagesConsumed < expectedMessages && System.currentTimeMillis() < endTime) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<String, String> record : records) {
                            try {
                                String[] parts = record.value().split(":", 2);
                                long sendTimeNanos = Long.parseLong(parts[0]);
                                long latencyNanos = System.nanoTime() - sendTimeNanos;
                                long latencyMicros = latencyNanos / 1000;
                                latencies.add(latencyMicros);
                                messagesConsumed++;

                                if (messagesConsumed % 1000 == 0) {
                                    System.out.printf("Consumer: Received %d messages%n", messagesConsumed);
                                }
                            } catch (Exception e) {
                                System.err.println("Error parsing message: " + e.getMessage());
                            }
                        }
                    }
                    System.out.println("Consumer: Consumed " + messagesConsumed + " messages.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String topic = "test-topic";
        int messageCount = 10000;
        long testDurationMs = 60000; // 60 seconds max

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch producerDone = new CountDownLatch(1);

        System.out.println("=== Concurrent End-to-End Latency Test ===");
        System.out.println("Starting producer and consumer concurrently...");

        // Start producer and consumer threads simultaneously
        ProducerThread producerThread = new ProducerThread(topic, messageCount, producerDone);
        ConsumerThread consumerThread = new ConsumerThread(topic, messageCount, testDurationMs, latencies, producerDone);

        long testStartTime = System.currentTimeMillis();
        producerThread.start();
        consumerThread.start();

        // Wait for both threads to complete
        producerThread.join();
        consumerThread.join();
        long testEndTime = System.currentTimeMillis();

        System.out.println();
        System.out.println("Test completed in " + (testEndTime - testStartTime) + " ms");
        System.out.println("Messages with latency data: " + latencies.size());

        if (!latencies.isEmpty()) {
            Collections.sort(latencies);
            long medianLatency = latencies.size() % 2 == 0 ?
                (latencies.get(latencies.size() / 2 - 1) + latencies.get(latencies.size() / 2)) / 2 :
                latencies.get(latencies.size() / 2);

            long minLatency = latencies.get(0);
            long maxLatency = latencies.get(latencies.size() - 1);
            long avgLatency = latencies.stream().mapToLong(Long::longValue).sum() / latencies.size();

            System.out.println("============================================");
            System.out.printf("Median End-to-End Latency: %d µs (%.3f ms)%n", medianLatency, medianLatency / 1000.0);
            System.out.printf("Min Latency: %d µs (%.3f ms)%n", minLatency, minLatency / 1000.0);
            System.out.printf("Max Latency: %d µs (%.3f ms)%n", maxLatency, maxLatency / 1000.0);
            System.out.printf("Avg Latency: %d µs (%.3f ms)%n", avgLatency, avgLatency / 1000.0);
            System.out.println("============================================");
        } else {
            System.out.println("No latency data collected!");
        }
    }
}

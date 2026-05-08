package com.lab.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ThroughputTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        // Test produce throughput
        testProduceThroughput();
        
        // Test consume throughput
        testConsumeThroughput();
    }

    private static void testProduceThroughput() throws IOException, InterruptedException {
        Properties properties = new Properties();
        try (InputStream input = ThroughputTest.class.getClassLoader().getResourceAsStream("producer.properties")) {
            if (input == null) {
                throw new IllegalStateException("Missing producer.properties");
            }
            properties.load(input);
        }

        properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String topic = properties.getProperty("topic", "throughput-test-topic");

        System.out.println("Testing Kafka maximum produce throughput with exponential load increase...");
        
        int targetThroughput = 1000; // Start with 1K msg/s
        int maxSuccessfulThroughput = 0;

        while (targetThroughput <= 1000000) {
            long testDurationMs = 5000; // 5 second test
            long intervalBetweenMessagesNs = 1_000_000_000L / targetThroughput;
            
            System.out.printf("\n--- Testing throughput: %,d msg/s ---\n", targetThroughput);

            AtomicLong sentCount = new AtomicLong(0);
            AtomicLong failedCount = new AtomicLong(0);
            CountDownLatch latch = new CountDownLatch(1);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                long startTime = System.nanoTime();
                long endTime = startTime + (testDurationMs * 1_000_000L);
                long nextSendTime = startTime;
                int messageNum = 0;

                while (System.nanoTime() < endTime && failedCount.get() < 10) { // Stop if 10+ failures
                    long currentTime = System.nanoTime();
                    
                    if (currentTime < nextSendTime) {
                        long sleepNs = nextSendTime - currentTime;
                        if (sleepNs > 1_000_000) {
                            Thread.sleep(sleepNs / 1_000_000);
                        } else {
                            while (System.nanoTime() < nextSendTime) {}
                        }
                    }

                    messageNum++;
                    String key = "key-" + messageNum;
                    String value = "msg-" + messageNum;
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            failedCount.incrementAndGet();
                        } else {
                            sentCount.incrementAndGet();
                        }
                    });

                    nextSendTime += intervalBetweenMessagesNs;
                }

                producer.flush();
                long actualThroughput = (sentCount.get() * 1000) / testDurationMs;
                System.out.printf("Result: Sent %,d messages | Failed: %,d | Actual throughput: %,d msg/s\n", 
                    sentCount.get(), failedCount.get(), actualThroughput);

                if (failedCount.get() == 0) {
                    maxSuccessfulThroughput = targetThroughput;
                    targetThroughput *= 2;
                } else {
                    System.out.println("Failures detected. Stopping throughput increase.");
                    break;
                }
            } catch (Exception e) {
                System.err.printf("Test failed: %s\n", e.getMessage());
                break;
            }
        }

        System.out.println("\n============================================");
        System.out.printf("Kafka Maximum Produce Throughput: %,d msg/s\n", maxSuccessfulThroughput);
        System.out.println("============================================");
    }

    private static void testConsumeThroughput() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = ThroughputTest.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            if (input == null) {
                throw new IllegalStateException("Missing consumer.properties");
            }
            properties.load(input);
        }

        properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        String topic = properties.getProperty("topic", "throughput-test-topic");

        System.out.println("\n\nTesting Kafka maximum consume throughput...");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("\n--- Consuming messages for 10 seconds ---\n");
            
            long startTime = System.currentTimeMillis();
            long endTime = startTime + 10000;
            long consumedCount = 0;

            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                consumedCount += records.count();
                
                if (consumedCount % 10000 == 0 && consumedCount > 0) {
                    System.out.printf("Consumed %,d messages\n", consumedCount);
                }
            }

            long consumeDuration = System.currentTimeMillis() - startTime;
            long maxConsumeThroughput = (consumedCount * 1000) / consumeDuration;

            System.out.println("\n============================================");
            System.out.printf("Kafka Maximum Consume Throughput: %,d msg/s\n", maxConsumeThroughput);
            System.out.println("============================================");
        }
    }
}

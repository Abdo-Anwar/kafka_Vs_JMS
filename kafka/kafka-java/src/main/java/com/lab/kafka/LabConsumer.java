package com.lab.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;

public class LabConsumer {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();

        try (InputStream input = LabConsumer.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            if (input == null) {
                throw new IllegalStateException("Missing consumer.properties in classpath.");
            }
            properties.load(input);
        }

        properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        String topic = properties.getProperty("topic", "test-topic");
        int expectedMessages = 10000;  // Must match producer count
        List<Long> latencies = new ArrayList<>();
        List<Long> consumeResponseTimes = new ArrayList<>();
        int messagesConsumed = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("Consumer subscribed to topic=%s, waiting for %d messages%n", topic, expectedMessages);

            while (messagesConsumed < expectedMessages) {
                long pollStartNanos = System.nanoTime();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                long pollTimeNanos = System.nanoTime() - pollStartNanos;
                
                for (ConsumerRecord<String, String> record : records) {
                    long pollTimeMicros = pollTimeNanos / 1000;
                    consumeResponseTimes.add(pollTimeMicros);
                    
                    // Parse embedded timestamp from value
                    String[] parts = record.value().split(":", 2);
                    long sendTimeNanos = Long.parseLong(parts[0]);
                    long latencyNanos = System.nanoTime() - sendTimeNanos;
                    long latencyMicros = latencyNanos / 1000;
                    latencies.add(latencyMicros);
                    messagesConsumed++;
                    
                    if (messagesConsumed % 1000 == 0) {
                        System.out.printf("Consumed msg %d (Avg latency: %.2f µs)%n", messagesConsumed,
                            latencies.stream().mapToLong(Long::longValue).average().orElse(0));
                    }
                }
            }

            // Calculate medians
            Collections.sort(latencies);
            Collections.sort(consumeResponseTimes);
            
            long medianLatency = latencies.size() % 2 == 0 ?
                (latencies.get(latencies.size() / 2 - 1) + latencies.get(latencies.size() / 2)) / 2 :
                latencies.get(latencies.size() / 2);
                
            long medianConsumeTime = consumeResponseTimes.size() % 2 == 0 ?
                (consumeResponseTimes.get(consumeResponseTimes.size() / 2 - 1) + 
                 consumeResponseTimes.get(consumeResponseTimes.size() / 2)) / 2 :
                consumeResponseTimes.get(consumeResponseTimes.size() / 2);

            System.out.println("============================================");
            System.out.printf("Results: %d messages consumed.%n", messagesConsumed);
            System.out.printf("Median Consume Response Time: %d µs%n", medianConsumeTime);
            System.out.printf("Median End-to-End Latency: %d µs%n", medianLatency);
            System.out.println("============================================");
        }
    }
}

package com.lab.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LabProducer {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();

        try (InputStream input = LabProducer.class.getClassLoader().getResourceAsStream("producer.properties")) {
            if (input == null) {
                throw new IllegalStateException("Missing producer.properties in classpath.");
            }
            properties.load(input);
        }

        properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String topic = properties.getProperty("topic", "test-topic");
        int messageCount = Integer.parseInt(properties.getProperty("message.count", "10000"));

        List<Long> producerResponseTimes = new ArrayList<>();
        AtomicInteger sentCount = new AtomicInteger(0);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 1; i <= messageCount; i++) {
                String key = "key-" + i;
                long sendTimeNanos = System.nanoTime();
                // Embed timestamp in value for latency calculation
                String value = sendTimeNanos + ":Kafka lab message " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                long sendStartNanos = System.nanoTime();
                Callback callback = (RecordMetadata metadata, Exception exception) -> {
                    long responseTimeNanos = System.nanoTime() - sendStartNanos;
                    long responseTimeMicros = responseTimeNanos / 1000;
                    producerResponseTimes.add(responseTimeMicros);
                    sentCount.incrementAndGet();
                    
                    if (exception != null) {
                        System.err.println("Failed to send message " + sentCount.get() + ": " + exception.getMessage());
                    }
                };

                producer.send(record, callback);
            }

            producer.flush();
            while (sentCount.get() < messageCount) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            System.out.println("All " + messageCount + " messages sent.");
            
            // Calculate median producer response time
            Collections.sort(producerResponseTimes);
            long medianProduceTime = producerResponseTimes.size() % 2 == 0 ?
                (producerResponseTimes.get(producerResponseTimes.size() / 2 - 1) + 
                 producerResponseTimes.get(producerResponseTimes.size() / 2)) / 2 :
                producerResponseTimes.get(producerResponseTimes.size() / 2);
            System.out.println("============================================");
            System.out.printf("Median Producer Response Time: %d µs%n", medianProduceTime);
            System.out.println("============================================");
        }
    }
}

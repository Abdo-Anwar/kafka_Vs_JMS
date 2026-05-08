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
        long runtimeSeconds = Long.parseLong(properties.getProperty("consumer.runtime.seconds", "30"));
        long endTime = System.currentTimeMillis() + (runtimeSeconds * 1000);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("Consumer subscribed to topic=%s and will run for %d seconds.%n", topic, runtimeSeconds);

            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "Consumed: topic=%s partition=%d offset=%d key=%s value=%s%n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()
                    );
                }
            }

            System.out.println("Consumer finished runtime window.");
        }
    }
}

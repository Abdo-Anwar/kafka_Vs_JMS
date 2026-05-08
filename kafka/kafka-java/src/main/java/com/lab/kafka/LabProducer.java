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
        int messageCount = Integer.parseInt(properties.getProperty("message.count", "10"));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 1; i <= messageCount; i++) {
                String key = "key-" + i;
                String value = "Kafka lab message " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                Callback callback = (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        System.err.println("Failed to send message: " + exception.getMessage());
                        return;
                    }
                    System.out.printf(
                            "Produced: topic=%s partition=%d offset=%d key=%s value=%s%n",
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            key,
                            value
                    );
                };

                producer.send(record, callback);
            }

            producer.flush();
            System.out.println("Producer completed successfully.");
        }
    }
}

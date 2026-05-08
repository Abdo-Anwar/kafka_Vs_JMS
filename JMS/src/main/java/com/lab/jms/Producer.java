package com.lab.jms;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import jakarta.jms.*;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Producer {
    public static void main(String[] args) throws Exception {
        // --- CONFIGURATION ---
        String brokerUrl = "tcp://localhost:61616";   // Artemis JMS port
        String queueName = "PERF_TEST_QUEUE";         // auto-created by Artemis
        int messageCount = 10000;                     // for performance tests
        byte[] payload = new byte[1024];              // 1 KB message body
        new Random().nextBytes(payload);              // fill with random data

        // --- CREATE CONNECTION AND SESSION ---
        ConnectionFactory factory = new ActiveMQJMSConnectionFactory(brokerUrl);
        try (Connection connection = factory.createConnection("admin", "admin");
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            // Destination (queue) – automatically created if not present
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // faster, no disk write

            List<Long> producerResponseTimes = new ArrayList<>();

            // --- SEND MESSAGES ---
            for (int i = 0; i < messageCount; i++) {
                // 1. Create a 1 KB binary message
                BytesMessage message = session.createBytesMessage();
                message.writeBytes(payload);

                // 2. Attach the current timestamp (in nanos) for later latency calculation
                long sendTimeNanos = System.nanoTime();
                message.setLongProperty("SEND_TIME_NANOS", sendTimeNanos);

                // 3. Measure the response time of the send() call (in nanos, convert to micros)
                long start = System.nanoTime();
                producer.send(message);
                long responseTimeNanos = System.nanoTime() - start;
                long responseTimeMicros = responseTimeNanos / 1000;
                producerResponseTimes.add(responseTimeMicros);

                // 4. Output every 1000th message for progress
                if ((i + 1) % 1000 == 0) {
                    System.out.printf("Sent msg %d | Avg response time: %.2f µs%n", i, 
                        producerResponseTimes.stream().mapToLong(Long::longValue).average().orElse(0));
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

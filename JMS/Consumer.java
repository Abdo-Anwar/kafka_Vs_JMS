package com.lab.jms;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import jakarta.jms.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Consumer {
    public static void main(String[] args) throws Exception {
        // --- CONFIGURATION ---
        String brokerUrl = "tcp://localhost:61616";
        String queueName = "PERF_TEST_QUEUE";
        int expectedMessages = 10000;  // must match producer count

        List<Long> latencies = new ArrayList<>();

        // --- CREATE CONNECTION AND SESSION ---
        ConnectionFactory factory = new ActiveMQJMSConnectionFactory(brokerUrl);
        try (Connection connection = factory.createConnection("admin", "admin");
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            connection.start(); // CRITICAL: starts message delivery

            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);

            System.out.println("Consumer ready. Waiting for messages...");

            // --- RECEIVE MESSAGES ---
            for (int i = 0; i < expectedMessages; i++) {
                // 1. Measure consume response time
                long start = System.currentTimeMillis();
                Message msg = consumer.receive(); // blocking call
                long consumeResponseTime = System.currentTimeMillis() - start;

                // 2. Calculate end-to-end latency
                long sendTime = msg.getLongProperty("SEND_TIME");
                long latency = System.currentTimeMillis() - sendTime;
                latencies.add(latency);

                // 3. Output progress
                System.out.printf("Received msg %d | Consume response time: %d ms | Latency: %d ms%n",
                        i, consumeResponseTime, latency);
            }

            // --- CALCULATE MEDIAN LATENCY ---
            Collections.sort(latencies);
            long median;
            int size = latencies.size();
            if (size % 2 == 0) {
                median = (latencies.get(size / 2 - 1) + latencies.get(size / 2)) / 2;
            } else {
                median = latencies.get(size / 2);
            }

            System.out.println("============================================");
            System.out.printf("Results: %d messages processed.%n", size);
            System.out.printf("Median latency: %d ms%n", median);
            System.out.println("============================================");
        }
    }
}
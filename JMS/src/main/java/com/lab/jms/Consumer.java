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
        List<Long> consumeResponseTimes = new ArrayList<>();

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
                // 1. Measure consume response time (in nanos, convert to micros)
                long start = System.nanoTime();
                Message msg = consumer.receive(); // blocking call
                long consumeResponseTimeNanos = System.nanoTime() - start;
                long consumeResponseTimeMicros = consumeResponseTimeNanos / 1000;
                consumeResponseTimes.add(consumeResponseTimeMicros);

                // 2. Calculate end-to-end latency (in nanos, convert to micros)
                long sendTimeNanos = msg.getLongProperty("SEND_TIME_NANOS");
                long latencyNanos = System.nanoTime() - sendTimeNanos;
                long latencyMicros = latencyNanos / 1000;
                latencies.add(latencyMicros);

                // 3. Output every 1000th message for progress
                if ((i + 1) % 1000 == 0) {
                    System.out.printf("Received msg %d (Avg latency: %.2f \u00b5s)%n", i,
                        latencies.stream().mapToLong(Long::longValue).average().orElse(0));
                }
            }

            // --- CALCULATE MEDIANS ---
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
            System.out.printf("Results: %d messages processed.%n", latencies.size());
            System.out.printf("Median Consume Response Time: %d \u00b5s%n", medianConsumeTime);
            System.out.printf("Median End-to-End Latency: %d \u00b5s%n", medianLatency);
            System.out.println("============================================");
        }
    }
}

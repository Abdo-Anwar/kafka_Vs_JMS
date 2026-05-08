package com.lab.jms;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import java.util.ArrayList;
import java.util.List;

public class ThroughputTest {
    public static void main(String[] args) throws Exception {
        String brokerURL = "tcp://localhost:61616";
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
        factory.setUser("admin");
        factory.setPassword("admin");

        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("THROUGHPUT_TEST_QUEUE");
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        System.out.println("Testing maximum produce throughput with exponential load increase...");
        
        int targetThroughput = 1000; // Start with 1K msg/s
        int maxSuccessfulThroughput = 0;

        while (targetThroughput <= 1000000) { // Test up to 1M msg/s
            long testDurationMs = 5000; // 5 second test
            long intervalBetweenMessagesNs = 1_000_000_000L / targetThroughput; // nanoseconds
            
            System.out.printf("\n--- Testing throughput: %,d msg/s (interval: %.2f µs) ---\n", 
                targetThroughput, intervalBetweenMessagesNs / 1000.0);

            long startTime = System.nanoTime();
            long endTime = startTime + (testDurationMs * 1_000_000L);
            long messageCount = 0;
            long failedCount = 0;
            long nextSendTime = startTime;

            try {
                while (System.nanoTime() < endTime) {
                    long currentTime = System.nanoTime();
                    
                    // Sleep until it's time to send next message
                    if (currentTime < nextSendTime) {
                        long sleepNs = nextSendTime - currentTime;
                        if (sleepNs > 1_000_000) { // Only sleep if > 1ms to avoid busy waiting
                            Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000));
                        } else {
                            // Busy wait for sub-millisecond precision
                            while (System.nanoTime() < nextSendTime) {
                                // Spin
                            }
                        }
                    }

                    try {
                        BytesMessage message = session.createBytesMessage();
                        message.writeBytes(new byte[1024]); // 1KB payload
                        producer.send(message);
                        messageCount++;
                    } catch (Exception e) {
                        failedCount++;
                        System.err.printf("Send failed: %s\n", e.getMessage());
                    }

                    nextSendTime += intervalBetweenMessagesNs;
                }

                long actualThroughput = (messageCount * 1000) / testDurationMs;
                System.out.printf("Result: Sent %,d messages | Failed: %,d | Actual throughput: %,d msg/s\n", 
                    messageCount, failedCount, actualThroughput);

                if (failedCount == 0) {
                    maxSuccessfulThroughput = targetThroughput;
                    targetThroughput *= 2; // Double the throughput
                } else {
                    // Test failed, stop increasing
                    System.out.println("Failures detected. Stopping throughput increase.");
                    break;
                }
            } catch (Exception e) {
                System.err.printf("Test failed at throughput %,d: %s\n", targetThroughput, e.getMessage());
                break;
            }
        }

        System.out.println("\n============================================");
        System.out.printf("JMS Maximum Produce Throughput: %,d msg/s\n", maxSuccessfulThroughput);
        System.out.println("============================================");

        // Now test consume throughput
        System.out.println("\n\nTesting maximum consume throughput...");
        
        // Create consumer
        MessageConsumer consumer = session.createConsumer(queue);
        
        System.out.printf("\n--- Consuming messages for 10 seconds ---\n");
        long startTime2 = System.currentTimeMillis();
        long endTime2 = startTime2 + 10000;
        long consumedCount = 0;

        while (System.currentTimeMillis() < endTime2) {
            Message msg = consumer.receiveNoWait();
            if (msg != null) {
                consumedCount++;
                if (consumedCount % 10000 == 0) {
                    System.out.printf("Consumed %,d messages\n", consumedCount);
                }
            } else {
                Thread.sleep(1); // Small sleep to avoid busy waiting
            }
        }

        long consumeDuration = System.currentTimeMillis() - startTime2;
        long maxConsumeThroughput = (consumedCount * 1000) / consumeDuration;

        System.out.println("\n============================================");
        System.out.printf("JMS Maximum Consume Throughput: %,d msg/s\n", maxConsumeThroughput);
        System.out.println("============================================");

        producer.close();
        consumer.close();
        session.close();
        connection.close();
    }
}

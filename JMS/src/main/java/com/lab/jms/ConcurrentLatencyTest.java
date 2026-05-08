package com.lab.jms;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import jakarta.jms.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class ConcurrentLatencyTest {
    static class ProducerThread extends Thread {
        private String brokerUrl;
        private String queueName;
        private int messageCount;
        private List<Long> latencies;
        private CountDownLatch producerDone;

        ProducerThread(String brokerUrl, String queueName, int messageCount,
                      List<Long> latencies, CountDownLatch producerDone) {
            this.brokerUrl = brokerUrl;
            this.queueName = queueName;
            this.messageCount = messageCount;
            this.latencies = latencies;
            this.producerDone = producerDone;
        }

        @Override
        public void run() {
            try {
                byte[] payload = new byte[1024];
                new Random().nextBytes(payload);

                ConnectionFactory factory = new ActiveMQJMSConnectionFactory(brokerUrl);
                try (Connection connection = factory.createConnection("admin", "admin");
                     Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

                    Destination destination = session.createQueue(queueName);
                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                    for (int i = 0; i < messageCount; i++) {
                        BytesMessage message = session.createBytesMessage();
                        message.writeBytes(payload);
                        long sendTimeNanos = System.nanoTime();
                        message.setLongProperty("SEND_TIME_NANOS", sendTimeNanos);
                        producer.send(message);

                        if ((i + 1) % 1000 == 0) {
                            System.out.printf("Producer: Sent %d messages%n", i + 1);
                        }
                    }
                    System.out.println("Producer: All " + messageCount + " messages sent.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                producerDone.countDown();
            }
        }
    }

    static class ConsumerThread extends Thread {
        private String brokerUrl;
        private String queueName;
        private int expectedMessages;
        private List<Long> latencies;
        private CountDownLatch producerDone;

        ConsumerThread(String brokerUrl, String queueName, int expectedMessages,
                      List<Long> latencies, CountDownLatch producerDone) {
            this.brokerUrl = brokerUrl;
            this.queueName = queueName;
            this.expectedMessages = expectedMessages;
            this.latencies = latencies;
            this.producerDone = producerDone;
        }

        @Override
        public void run() {
            try {
                ConnectionFactory factory = new ActiveMQJMSConnectionFactory(brokerUrl);
                try (Connection connection = factory.createConnection("admin", "admin");
                     Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

                    connection.start();
                    Destination destination = session.createQueue(queueName);
                    MessageConsumer consumer = session.createConsumer(destination);
                    System.out.println("Consumer: Ready to receive messages...");

                    int messagesConsumed = 0;
                    long timeoutMs = 60000; // 60 second timeout
                    long startTime = System.currentTimeMillis();

                    while (messagesConsumed < expectedMessages && 
                           System.currentTimeMillis() - startTime < timeoutMs) {
                        Message msg = consumer.receive(1000); // 1 second timeout per receive
                        if (msg != null) {
                            long sendTimeNanos = msg.getLongProperty("SEND_TIME_NANOS");
                            long latencyNanos = System.nanoTime() - sendTimeNanos;
                            long latencyMicros = latencyNanos / 1000;
                            latencies.add(latencyMicros);
                            messagesConsumed++;

                            if (messagesConsumed % 1000 == 0) {
                                System.out.printf("Consumer: Received %d messages%n", messagesConsumed);
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

    public static void main(String[] args) throws Exception {
        String brokerUrl = "tcp://localhost:61616";
        String queueName = "PERF_TEST_QUEUE_CONCURRENT";
        int messageCount = 10000;

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch producerDone = new CountDownLatch(1);

        System.out.println("=== JMS Concurrent End-to-End Latency Test ===");
        System.out.println("Starting producer and consumer concurrently...\n");

        long testStartTime = System.currentTimeMillis();
        ProducerThread producerThread = new ProducerThread(brokerUrl, queueName, messageCount, latencies, producerDone);
        ConsumerThread consumerThread = new ConsumerThread(brokerUrl, queueName, messageCount, latencies, producerDone);

        producerThread.start();
        consumerThread.start();

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

package com.lab.jms;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import jakarta.jms.*;
import java.util.Random;

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

            // --- SEND MESSAGES ---
            for (int i = 0; i < messageCount; i++) {
                // 1. Create a 1 KB binary message
                BytesMessage message = session.createBytesMessage();
                message.writeBytes(payload);

                // 2. Attach the current timestamp for later latency calculation
                message.setLongProperty("SEND_TIME", System.currentTimeMillis());

                // 3. Measure the response time of the send() call
                long start = System.currentTimeMillis();
                producer.send(message);
                long responseTime = System.currentTimeMillis() - start;

                // 4. Output progress (Member 4 can collect these values)
                System.out.printf("Sent msg %d | Producer response time: %d ms%n", i, responseTime);
            }
            System.out.println("All " + messageCount + " messages sent.");
        }
    }
}
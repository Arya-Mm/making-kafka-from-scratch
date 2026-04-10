package com.simplekafka.broker;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class SimpleKafkaProducer {

    private final SimpleKafkaClient client;
    private final String topic;
    private final Random random = new Random();

    public SimpleKafkaProducer(String host, int port, String topic) {
        this.client = new SimpleKafkaClient(host, port);
        this.topic = topic;
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    // Send to random partition
    public long send(String message) throws IOException {

        int partition = random.nextInt(3); // simple assumption

        byte[] data = message.getBytes(StandardCharsets.UTF_8);

        return client.send(topic, partition, data);
    }

    // Send to specific partition
    public long send(String message, int partition) throws IOException {

        byte[] data = message.getBytes(StandardCharsets.UTF_8);

        return client.send(topic, partition, data);
    }
}
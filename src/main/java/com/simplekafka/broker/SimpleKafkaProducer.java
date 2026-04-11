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

    public long send(String message) throws IOException {

        int partitionCount = client.getPartitionCount(topic);
        int partition = partitionCount > 0 ? random.nextInt(partitionCount) : 0;

        byte[] data = message.getBytes(StandardCharsets.UTF_8);

        return client.send(topic, partition, data);
    }

    public long send(String message, int partition) throws IOException {

        byte[] data = message.getBytes(StandardCharsets.UTF_8);

        return client.send(topic, partition, data);
    }
}
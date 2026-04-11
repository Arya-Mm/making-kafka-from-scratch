package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class SimpleKafkaProducer {

    private final SimpleKafkaClient client;
    private final String topic;
    private final Random random = new Random();

    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
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

    public void close() {
        // no resources to close in this minimal implementation
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: java com.simplekafka.client.SimpleKafkaProducer <bootstrapHost> <bootstrapPort> <topic>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];

        SimpleKafkaProducer producer = new SimpleKafkaProducer(host, port, topic);
        producer.initialize();

        for (int i = 0; i < 10; i++) {
            long offset = producer.send("msg-" + i);
            System.out.println("Produced at offset: " + offset);
        }

        producer.close();
    }
}
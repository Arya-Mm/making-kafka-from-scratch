package com.simplekafka.broker;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleKafkaConsumer {

    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;

    private long currentOffset = 0;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    public SimpleKafkaConsumer(String host, int port, String topic, int partition) {
        this.client = new SimpleKafkaClient(host, port);
        this.topic = topic;
        this.partition = partition;
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    public List<byte[]> poll() throws IOException {

        List<byte[]> messages =
                client.fetch(topic, partition, currentOffset, 10);

        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }

        return messages;
    }

    // 🔥 CONTINUOUS CONSUMPTION
    public void startConsuming(MessageHandler handler) {

        if (running.compareAndSet(false, true)) {

            consumerThread = new Thread(() -> {

                while (running.get()) {
                    try {

                        List<byte[]> messages = poll();

                        for (byte[] msg : messages) {
                            handler.handle(new String(msg));
                        }

                        if (messages.isEmpty()) {
                            Thread.sleep(500);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        running.set(false);
                    }
                }
            });

            consumerThread.start();

            System.out.println("Consumer started...");
        }
    }

    public void stop() {
        running.set(false);
    }

    // handler interface
    public interface MessageHandler {
        void handle(String message);
    }
}
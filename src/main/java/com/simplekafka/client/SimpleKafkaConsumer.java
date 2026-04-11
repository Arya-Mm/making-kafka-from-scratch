package com.simplekafka.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleKafkaConsumer {

    private static final int MAX_BYTES = 1024 * 1024;
    private static final long POLL_INTERVAL_MS = 500;

    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;

    private long currentOffset = 0;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
    }

    public void initialize() throws IOException {
        client.initialize();
    }

    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, 10);

        if (!messages.isEmpty()) {
            currentOffset += messages.size();
        }

        return messages;
    }

    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        List<byte[]> messages = poll();
                        long startOffset = currentOffset - messages.size();
                        for (int i = 0; i < messages.size(); i++) {
                            handler.handle(messages.get(i), startOffset + i);
                        }

                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        running.set(false);
                    }
                }
            });

            consumerThread.setDaemon(true);
            consumerThread.start();
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false) && consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void seek(long offset) {
        this.currentOffset = offset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public interface MessageHandler {
        void handle(byte[] message, long offset);
    }
}
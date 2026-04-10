package com.simplekafka.broker;

import java.nio.ByteBuffer;

public class Protocol {

    // =========================
    // MESSAGE TYPES
    // =========================

    // Requests
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Responses
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte ERROR_RESPONSE = 0x13;

    // =========================
    // ENCODE REQUESTS
    // =========================

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes();

        int size = 1 + 2 + topicBytes.length + 4 + 4 + message.length;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        byte[] topicBytes = topic.getBytes();

        int size = 1 + 2 + topicBytes.length + 4 + 8 + 4;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int partitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes();

        int size = 1 + 2 + topicBytes.length + 4 + 2;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partitions);
        buffer.putShort(replicationFactor);

        buffer.flip();
        return buffer;
    }

    // =========================
    // DECODE RESPONSES
    // =========================

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte type = buffer.get();

        if (type != PRODUCE_RESPONSE) {
            return new ProduceResult(-1, "Invalid response type");
        }

        long offset = buffer.getLong();
        return new ProduceResult(offset, null);
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte type = buffer.get();

        if (type != FETCH_RESPONSE) {
            return new FetchResult(null, "Invalid response type");
        }

        int count = buffer.getInt();
        byte[][] messages = new byte[count][];

        for (int i = 0; i < count; i++) {
            int size = buffer.getInt();
            byte[] msg = new byte[size];
            buffer.get(msg);
            messages[i] = msg;
        }

        return new FetchResult(messages, null);
    }

    // =========================
    // ENCODE RESPONSES
    // =========================

    public static ByteBuffer encodeProduceResponse(long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8);

        buffer.put(PRODUCE_RESPONSE);
        buffer.putLong(offset);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchResponse(byte[][] messages) {
        int totalSize = 1 + 4;

        for (byte[] msg : messages) {
            totalSize += 4 + msg.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.put(FETCH_RESPONSE);
        buffer.putInt(messages.length);

        for (byte[] msg : messages) {
            buffer.putInt(msg.length);
            buffer.put(msg);
        }

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeErrorResponse(String errorMessage) {
        byte[] errorBytes = errorMessage.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + errorBytes.length);

        buffer.put(ERROR_RESPONSE);
        buffer.putInt(errorBytes.length);
        buffer.put(errorBytes);

        buffer.flip();
        return buffer;
    }

    // =========================
    // RESULT CLASSES
    // =========================

    public static class ProduceResult {
        public final long offset;
        public final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }
    }

    public static class FetchResult {
        public final byte[][] messages;
        public final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }
    }

    public static class MetadataResult {
        public final String error;

        public MetadataResult(String error) {
            this.error = error;
        }
    }
}
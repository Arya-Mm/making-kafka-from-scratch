package com.simplekafka.broker;

import java.nio.ByteBuffer;

public class Protocol {

    // REQUEST TYPES
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte REPLICATE = 0x05;

    // RESPONSE TYPES
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte ERROR = 0x7F;

    // ================= PRODUCE =================
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

    public static ByteBuffer encodeProduceResponse(long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8);
        buffer.put(PRODUCE_RESPONSE);
        buffer.putLong(offset);
        buffer.flip();
        return buffer;
    }

    // ================= FETCH =================
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxMessages, String groupId) {

        byte[] topicBytes = topic.getBytes();
        byte[] groupBytes = groupId.getBytes();

        int size = 1 + 2 + topicBytes.length + 4 + 8 + 4 + 2 + groupBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxMessages);
        buffer.putShort((short) groupBytes.length);
        buffer.put(groupBytes);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchResponse(byte[][] messages) {

        int size = 1 + 4;

        for (byte[] msg : messages) {
            size += 4 + msg.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(FETCH_RESPONSE);
        buffer.putInt(messages.length);

        for (byte[] msg : messages) {
            buffer.putInt(msg.length);
            buffer.put(msg);
        }

        buffer.flip();
        return buffer;
    }

    // ================= METADATA =================
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataResponse() {

        String topic = "test";
        byte[] topicBytes = topic.getBytes();

        int partitions = 3;
        int topicCount = 1;

        int size = 1 + 4 + 2 + topicBytes.length + 4 + (partitions * 8);

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(METADATA_RESPONSE);
        buffer.putInt(topicCount);

        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.putInt(partitions);

        for (int i = 0; i < partitions; i++) {
            buffer.putInt(i);
            buffer.putInt(ClusterManager.getLeader(i));
        }

        buffer.flip();
        return buffer;
    }

    // ================= REPLICATION =================
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {

        byte[] topicBytes = topic.getBytes();

        int size = 1 + 2 + topicBytes.length + 4 + 8 + 4 + message.length;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    // ================= ERROR =================
    public static ByteBuffer encodeError(String msg) {

        byte[] bytes = msg.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + bytes.length);

        buffer.put(ERROR);
        buffer.putInt(bytes.length);
        buffer.put(bytes);

        buffer.flip();
        return buffer;
    }
}
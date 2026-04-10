package com.simplekafka.broker;

import java.nio.ByteBuffer;

public class Protocol {

    // ================= REQUEST TYPES =================
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;

    // 🔥 NEW
    public static final byte REPLICATE = 0x05;

    // ================= RESPONSE TYPES =================
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte ERROR_RESPONSE = 0x13;

    // =================================================
    // 🔥 PRODUCE REQUEST
    // =================================================
    public static ByteBuffer encodeProduceRequest(
            String topic,
            int partition,
            byte[] message) {

        byte[] topicBytes = topic.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 + 2 + topicBytes.length + 4 + 4 + message.length
        );

        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    // =================================================
    // 🔥 PRODUCE RESPONSE
    // =================================================
    public static ByteBuffer encodeProduceResponse(long offset) {

        ByteBuffer buffer = ByteBuffer.allocate(1 + 8);

        buffer.put(PRODUCE_RESPONSE);
        buffer.putLong(offset);

        buffer.flip();
        return buffer;
    }

    // =================================================
    // 🔥 FETCH REQUEST (WITH CONSUMER GROUP)
    // =================================================
    public static ByteBuffer encodeFetchRequest(
            String topic,
            int partition,
            long offset,
            int maxMessages,
            String groupId) {

        byte[] topicBytes = topic.getBytes();
        byte[] groupBytes = groupId.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 +
                2 + topicBytes.length +
                4 +
                8 +
                4 +
                2 + groupBytes.length
        );

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

    // =================================================
    // 🔥 FETCH RESPONSE
    // =================================================
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

    // =================================================
    // 🔥 REPLICATION REQUEST
    // =================================================
    public static ByteBuffer encodeReplicateRequest(
            String topic,
            int partition,
            long offset,
            byte[] message) {

        byte[] topicBytes = topic.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 +
                2 + topicBytes.length +
                4 +
                8 +
                4 + message.length
        );

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

    // =================================================
    // 🔥 ERROR RESPONSE
    // =================================================
    public static ByteBuffer encodeError(String errorMessage) {

        byte[] errorBytes = errorMessage.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(
                1 + 2 + errorBytes.length
        );

        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorBytes.length);
        buffer.put(errorBytes);

        buffer.flip();
        return buffer;
    }
}
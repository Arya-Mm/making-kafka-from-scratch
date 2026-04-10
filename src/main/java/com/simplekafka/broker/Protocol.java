package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Protocol {

    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;

    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte ERROR_RESPONSE = 0x7F;

    // PRODUCE
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

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

    public static ByteBuffer encodeProduceResponse(long offset) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 8);
        buffer.put(PRODUCE_RESPONSE);
        buffer.putLong(offset);
        buffer.flip();
        return buffer;
    }

    // FETCH WITH GROUP
    public static ByteBuffer encodeFetchRequest(
            String topic,
            int partition,
            long offset,
            int maxMessages,
            String groupId
    ) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.allocate(
                1 + 2 + topicBytes.length + 4 + 8 + 4 + 2 + groupBytes.length
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

    public static ByteBuffer encodeError(String errorMsg) {
        byte[] err = errorMsg.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + err.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putInt(err.length);
        buffer.put(err);

        buffer.flip();
        return buffer;
    }
}
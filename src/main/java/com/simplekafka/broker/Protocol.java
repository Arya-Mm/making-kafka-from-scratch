package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Protocol {

    // REQUEST TYPES
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte REPLICATE = 0x05;
    public static final byte TOPIC_NOTIFICATION = 0x06;

    // RESPONSE TYPES
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
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

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType == ERROR) {
            return new ProduceResult(-1, decodeError(buffer));
        }
        if (responseType != PRODUCE_RESPONSE) {
            throw new IllegalArgumentException("Unexpected response type: " + responseType);
        }
        long offset = buffer.getLong();
        return new ProduceResult(offset, null);
    }

    // ================= FETCH =================
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxMessages, String groupId) {
        byte[] topicBytes = topic.getBytes();
        byte[] groupBytes = groupId == null ? new byte[0] : groupId.getBytes();

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

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxMessages) {
        return encodeFetchRequest(topic, partition, offset, maxMessages, "");
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType == ERROR) {
            return new FetchResult(null, decodeError(buffer));
        }
        if (responseType != FETCH_RESPONSE) {
            throw new IllegalArgumentException("Unexpected response type: " + responseType);
        }

        int count = buffer.getInt();
        List<byte[]> messages = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            int len = buffer.getInt();
            byte[] message = new byte[len];
            buffer.get(message);
            messages.add(message);
        }

        return new FetchResult(messages, null);
    }

    public static ByteBuffer encodeFetchResponse(byte[][] messages) {
        int size = 1 + 4;
        for (byte[] message : messages) {
            size += 4 + message.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(FETCH_RESPONSE);
        buffer.putInt(messages.length);

        for (byte[] message : messages) {
            buffer.putInt(message.length);
            buffer.put(message);
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

    public static ByteBuffer encodeMetadataResponse(List<BrokerInfo> brokers, Map<String, Map<Integer, Integer>> topicLeaders) {
        int size = 1 + 4;
        for (BrokerInfo broker : brokers) {
            byte[] hostBytes = broker.getHost().getBytes();
            size += 4 + 2 + hostBytes.length + 4;
        }

        size += 4;
        for (Map.Entry<String, Map<Integer, Integer>> topicEntry : topicLeaders.entrySet()) {
            byte[] topicBytes = topicEntry.getKey().getBytes();
            size += 2 + topicBytes.length + 4;
            size += topicEntry.getValue().size() * (4 + 4);
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(METADATA_RESPONSE);
        buffer.putInt(brokers.size());

        for (BrokerInfo broker : brokers) {
            byte[] hostBytes = broker.getHost().getBytes();
            buffer.putInt(broker.getId());
            buffer.putShort((short) hostBytes.length);
            buffer.put(hostBytes);
            buffer.putInt(broker.getPort());
        }

        buffer.putInt(topicLeaders.size());
        for (Map.Entry<String, Map<Integer, Integer>> topicEntry : topicLeaders.entrySet()) {
            byte[] topicBytes = topicEntry.getKey().getBytes();
            buffer.putShort((short) topicBytes.length);
            buffer.put(topicBytes);
            buffer.putInt(topicEntry.getValue().size());

            for (Map.Entry<Integer, Integer> partitionEntry : topicEntry.getValue().entrySet()) {
                buffer.putInt(partitionEntry.getKey());
                buffer.putInt(partitionEntry.getValue());
            }
        }

        buffer.flip();
        return buffer;
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType == ERROR) {
            return new MetadataResult(null, null, decodeError(buffer));
        }
        if (responseType != METADATA_RESPONSE) {
            throw new IllegalArgumentException("Unexpected response type: " + responseType);
        }

        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>(brokerCount);
        for (int i = 0; i < brokerCount; i++) {
            int id = buffer.getInt();
            short hostLen = buffer.getShort();
            byte[] hostBytes = new byte[hostLen];
            buffer.get(hostBytes);
            String host = new String(hostBytes);
            int port = buffer.getInt();
            brokers.add(new BrokerInfo(id, host, port));
        }

        int topicCount = buffer.getInt();
        Map<String, Map<Integer, Integer>> topicLeaders = new HashMap<>();
        for (int i = 0; i < topicCount; i++) {
            short topicLen = buffer.getShort();
            byte[] topicBytes = new byte[topicLen];
            buffer.get(topicBytes);
            String topic = new String(topicBytes);
            int partitionCount = buffer.getInt();

            Map<Integer, Integer> partitionMap = new HashMap<>();
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leader = buffer.getInt();
                partitionMap.put(partitionId, leader);
            }
            topicLeaders.put(topic, partitionMap);
        }

        return new MetadataResult(brokers, topicLeaders, null);
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        byte[] topicBytes = topic.getBytes();
        int size = 1 + 2 + topicBytes.length + 4 + 2;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicResponse(boolean success, String message) {
        byte[] messageBytes = message.getBytes();
        int size = 1 + 1 + 4 + messageBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(CREATE_TOPIC_RESPONSE);
        buffer.put((byte) (success ? 1 : 0));
        buffer.putInt(messageBytes.length);
        buffer.put(messageBytes);
        buffer.flip();
        return buffer;
    }

    public static CreateTopicResult decodeCreateTopicResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType == ERROR) {
            return new CreateTopicResult(false, decodeError(buffer));
        }
        if (responseType != CREATE_TOPIC_RESPONSE) {
            throw new IllegalArgumentException("Unexpected response type: " + responseType);
        }

        boolean success = buffer.get() == 1;
        int messageLength = buffer.getInt();
        byte[] messageBytes = new byte[messageLength];
        buffer.get(messageBytes);
        return new CreateTopicResult(success, new String(messageBytes));
    }

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

    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes();
        int size = 1 + 2 + topicBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeError(String msg) {
        byte[] bytes = msg.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + bytes.length);
        buffer.put(ERROR);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        channel.write(encodeError(errorMessage));
    }

    private static String decodeError(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }

    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long offset() {
            return offset;
        }

        public String error() {
            return error;
        }
    }

    public static class FetchResult {
        private final List<byte[]> messages;
        private final String error;

        public FetchResult(List<byte[]> messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public List<byte[]> messages() {
            return messages;
        }

        public String error() {
            return error;
        }
    }

    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final Map<String, Map<Integer, Integer>> topicLeaders;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, Map<String, Map<Integer, Integer>> topicLeaders, String error) {
            this.brokers = brokers;
            this.topicLeaders = topicLeaders;
            this.error = error;
        }

        public List<BrokerInfo> brokers() {
            return brokers;
        }

        public Map<String, Map<Integer, Integer>> topicLeaders() {
            return topicLeaders;
        }

        public String error() {
            return error;
        }
    }

    public static class CreateTopicResult {
        private final boolean success;
        private final String message;

        public CreateTopicResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public boolean success() {
            return success;
        }

        public String message() {
            return message;
        }
    }
}
package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

public class SimpleKafkaClient {

    private final String bootstrapHost;
    private final int bootstrapPort;

    private final Map<String, Map<Integer, BrokerInfo>> metadata = new HashMap<>();
    private final Map<Integer, BrokerInfo> brokers = new HashMap<>();

    public SimpleKafkaClient(String host, int port) {
        this.bootstrapHost = host;
        this.bootstrapPort = port;
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    public void refreshMetadata() throws IOException {

        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(bootstrapHost, bootstrapPort));

            ByteBuffer request = Protocol.encodeMetadataRequest();
            socket.write(request);

            ByteBuffer response = ByteBuffer.allocate(4096);
            socket.read(response);
            response.flip();

            byte type = response.get();
            if (type != Protocol.METADATA_RESPONSE) {
                throw new RuntimeException("Invalid metadata response");
            }

            brokers.clear();
            int brokerCount = response.getInt();
            for (int i = 0; i < brokerCount; i++) {
                int id = response.getInt();
                short hostLen = response.getShort();
                byte[] hostBytes = new byte[hostLen];
                response.get(hostBytes);
                String host = new String(hostBytes);
                int port = response.getInt();
                brokers.put(id, new BrokerInfo(id, host, port));
            }

            metadata.clear();
            int topicCount = response.getInt();
            for (int i = 0; i < topicCount; i++) {
                short topicLen = response.getShort();
                byte[] topicBytes = new byte[topicLen];
                response.get(topicBytes);

                String topic = new String(topicBytes);
                int partitionCount = response.getInt();

                Map<Integer, BrokerInfo> partitionMap = new HashMap<>();
                for (int j = 0; j < partitionCount; j++) {
                    int partitionId = response.getInt();
                    int leader = response.getInt();
                    BrokerInfo leaderInfo = brokers.get(leader);
                    if (leaderInfo != null) {
                        partitionMap.put(partitionId, leaderInfo);
                    }
                }
                metadata.put(topic, partitionMap);
            }

            System.out.println("Metadata loaded: " + metadata);
        }
    }

    public long send(String topic, int partition, byte[] message) throws IOException {
        if (!metadata.containsKey(topic)) {
            refreshMetadata();
            if (!metadata.containsKey(topic)) {
                createTopic(topic, 3, (short) 1);
                refreshMetadata();
            }
        }

        BrokerInfo leader = getLeader(topic, partition);
        SocketChannel socket = SocketChannel.open();
        socket.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

        ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
        socket.write(request);

        ByteBuffer response = ByteBuffer.allocate(1024);
        socket.read(response);
        response.flip();

        byte type = response.get();
        if (type != Protocol.PRODUCE_RESPONSE) {
            throw new RuntimeException("Produce failed");
        }

        long offset = response.getLong();
        socket.close();
        return offset;
    }

    public java.util.List<byte[]> fetch(String topic, int partition, long offset, int maxMessages) throws IOException {
        if (!metadata.containsKey(topic)) {
            refreshMetadata();
        }

        BrokerInfo leader = getLeader(topic, partition);
        SocketChannel socket = SocketChannel.open();
        socket.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

        ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxMessages, "group1");
        socket.write(request);

        ByteBuffer response = ByteBuffer.allocate(4096);
        socket.read(response);
        response.flip();

        byte type = response.get();
        if (type != Protocol.FETCH_RESPONSE) {
            throw new RuntimeException("Fetch failed");
        }

        int count = response.getInt();
        java.util.List<byte[]> messages = new java.util.ArrayList<>();

        for (int i = 0; i < count; i++) {
            int len = response.getInt();
            byte[] msg = new byte[len];
            response.get(msg);
            messages.add(msg);
        }

        socket.close();
        return messages;
    }

    public int getPartitionCount(String topic) {
        return metadata.getOrDefault(topic, Map.of()).size();
    }

    public void createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(bootstrapHost, bootstrapPort));

            ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
            socket.write(request);

            ByteBuffer response = ByteBuffer.allocate(1024);
            socket.read(response);
            response.flip();

            byte type = response.get();
            if (type != Protocol.CREATE_TOPIC_RESPONSE) {
                throw new RuntimeException("CreateTopic failed");
            }

            boolean success = response.get() == 1;
            int messageLength = response.getInt();
            byte[] messageBytes = new byte[messageLength];
            response.get(messageBytes);

            if (!success) {
                throw new RuntimeException(new String(messageBytes));
            }
        }
    }

    private BrokerInfo getLeader(String topic, int partition) {
        if (!metadata.containsKey(topic)) {
            throw new RuntimeException("Unknown topic");
        }

        Map<Integer, BrokerInfo> partitions = metadata.get(topic);
        if (!partitions.containsKey(partition)) {
            throw new RuntimeException("Unknown partition");
        }

        return partitions.get(partition);
    }
}
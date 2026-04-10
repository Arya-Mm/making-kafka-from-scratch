package com.simplekafka.client;

import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class SimpleKafkaClient {

    private final String bootstrapHost;
    private final int bootstrapPort;

    // topic -> partition -> leader broker
    private final Map<String, Map<Integer, Integer>> metadata = new HashMap<>();

    public SimpleKafkaClient(String host, int port) {
        this.bootstrapHost = host;
        this.bootstrapPort = port;
    }

    // ================= INIT =================
    public void initialize() throws IOException {
        refreshMetadata();
    }

    // ================= METADATA =================
    public void refreshMetadata() throws IOException {

        SocketChannel socket = SocketChannel.open();
        socket.connect(new InetSocketAddress(bootstrapHost, bootstrapPort));

        ByteBuffer request = Protocol.encodeMetadataRequest();
        socket.write(request);

        ByteBuffer response = ByteBuffer.allocate(1024);
        socket.read(response);
        response.flip();

        byte type = response.get();

        if (type != Protocol.METADATA_RESPONSE) {
            throw new RuntimeException("Invalid metadata response");
        }

        int topicCount = response.getInt();

        for (int i = 0; i < topicCount; i++) {

            short topicLen = response.getShort();
            byte[] topicBytes = new byte[topicLen];
            response.get(topicBytes);

            String topic = new String(topicBytes);

            int partitionCount = response.getInt();

            Map<Integer, Integer> partitionMap = new HashMap<>();

            for (int j = 0; j < partitionCount; j++) {

                int partitionId = response.getInt();
                int leader = response.getInt();

                partitionMap.put(partitionId, leader);
            }

            metadata.put(topic, partitionMap);
        }

        socket.close();

        System.out.println("Metadata loaded: " + metadata);
    }

    // ================= PRODUCE =================
    public long send(String topic, int partition, byte[] message) throws IOException {

        int leader = getLeader(topic, partition);

        SocketChannel socket = SocketChannel.open();
        socket.connect(new InetSocketAddress("localhost", 9092 + leader));

        ByteBuffer request = Protocol.encodeProduceRequest(
                topic,
                partition,
                message
        );

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

    // ================= FETCH =================
    public List<byte[]> fetch(String topic, int partition, long offset, int maxMessages) throws IOException {

        int leader = getLeader(topic, partition);

        SocketChannel socket = SocketChannel.open();
        socket.connect(new InetSocketAddress("localhost", 9092 + leader));

        ByteBuffer request = Protocol.encodeFetchRequest(
                topic,
                partition,
                offset,
                maxMessages,
                "group1"
        );

        socket.write(request);

        ByteBuffer response = ByteBuffer.allocate(4096);
        socket.read(response);
        response.flip();

        byte type = response.get();

        if (type != Protocol.FETCH_RESPONSE) {
            throw new RuntimeException("Fetch failed");
        }

        int count = response.getInt();
        List<byte[]> messages = new ArrayList<>();

        for (int i = 0; i < count; i++) {

            int len = response.getInt();
            byte[] msg = new byte[len];
            response.get(msg);

            messages.add(msg);
        }

        socket.close();

        return messages;
    }

    // ================= HELPER =================
    private int getLeader(String topic, int partition) {

        if (!metadata.containsKey(topic)) {
            throw new RuntimeException("Unknown topic");
        }

        Map<Integer, Integer> partitions = metadata.get(topic);

        if (!partitions.containsKey(partition)) {
            throw new RuntimeException("Unknown partition");
        }

        return partitions.get(partition);
    }
}
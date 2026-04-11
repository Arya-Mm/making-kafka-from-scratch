package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKafkaClient {

    private final String bootstrapHost;
    private final int bootstrapPort;

    private final Map<String, TopicMetadata> topicMetadata = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();

    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapHost = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
    }

    public void initialize() throws IOException {
        refreshMetadata();
    }

    public void refreshMetadata() throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(bootstrapHost, bootstrapPort));
            socket.write(Protocol.encodeMetadataRequest());

            ByteBuffer response = ByteBuffer.allocate(8192);
            int bytesRead = socket.read(response);
            if (bytesRead <= 0) {
                throw new IOException("Empty metadata response");
            }

            response.flip();
            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response);

            if (result.error() != null && !result.error().isBlank()) {
                throw new IOException(result.error());
            }

            brokers.clear();
            for (BrokerInfo broker : result.brokers()) {
                brokers.put(broker.getId(), broker);
            }

            topicMetadata.clear();
            for (Map.Entry<String, Map<Integer, Integer>> entry : result.topicLeaders().entrySet()) {
                Map<Integer, BrokerInfo> partitionLeaders = new HashMap<>();
                for (Map.Entry<Integer, Integer> partitionEntry : entry.getValue().entrySet()) {
                    BrokerInfo leader = brokers.get(partitionEntry.getValue());
                    if (leader != null) {
                        partitionLeaders.put(partitionEntry.getKey(), leader);
                    }
                }
                topicMetadata.put(entry.getKey(), new TopicMetadata(entry.getKey(), partitionLeaders));
            }
        }
    }

    public long send(String topic, int partition, byte[] message) throws IOException {
        ensureTopicMetadata(topic);

        BrokerInfo leader = getLeader(topic, partition);

        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));
            socket.write(Protocol.encodeProduceRequest(topic, partition, message));

            ByteBuffer response = ByteBuffer.allocate(1024);
            int bytesRead = socket.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No response from broker");
            }

            response.flip();
            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);

            if (result.error() != null) {
                throw new IOException(result.error());
            }

            return result.offset();
        }
    }

    public java.util.List<byte[]> fetch(String topic, int partition, long offset, int maxMessages) throws IOException {
        ensureTopicMetadata(topic);

        BrokerInfo leader = getLeader(topic, partition);

        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));
            socket.write(Protocol.encodeFetchRequest(topic, partition, offset, maxMessages, "default"));

            ByteBuffer response = ByteBuffer.allocate(8192);
            int bytesRead = socket.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No response from broker");
            }

            response.flip();
            Protocol.FetchResult result = Protocol.decodeFetchResponse(response);

            if (result.error() != null) {
                throw new IOException(result.error());
            }

            return result.messages();
        }
    }

    public java.util.List<Protocol.FetchMessage> fetchWithOffsets(String topic, int partition, long offset, int maxMessages) throws IOException {
        ensureTopicMetadata(topic);

        BrokerInfo leader = getLeader(topic, partition);

        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));
            socket.write(Protocol.encodeFetchRequest(topic, partition, offset, maxMessages, "default"));

            ByteBuffer response = ByteBuffer.allocate(8192);
            int bytesRead = socket.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No response from broker");
            }

            response.flip();
            Protocol.FetchResult result = Protocol.decodeFetchResponse(response);

            if (result.error() != null) {
                throw new IOException(result.error());
            }

            return result.records();
        }
    }

    public int getPartitionCount(String topic) {
        TopicMetadata metadata = topicMetadata.get(topic);
        return metadata == null ? 0 : metadata.partitions().size();
    }

    public void createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(bootstrapHost, bootstrapPort));
            socket.write(Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor));

            ByteBuffer response = ByteBuffer.allocate(1024);
            int bytesRead = socket.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No response from broker");
            }

            response.flip();
            Protocol.CreateTopicResult result = Protocol.decodeCreateTopicResponse(response);

            if (!result.success()) {
                throw new IOException(result.message());
            }
        }
    }

    private BrokerInfo getLeader(String topic, int partition) {
        TopicMetadata metadata = topicMetadata.get(topic);
        if (metadata == null) {
            throw new IllegalStateException("Unknown topic: " + topic);
        }

        BrokerInfo leader = metadata.partitions().get(partition);
        if (leader == null) {
            throw new IllegalStateException("Unknown partition " + partition + " for topic " + topic);
        }

        return leader;
    }

    private void ensureTopicMetadata(String topic) throws IOException {
        if (topicMetadata.containsKey(topic)) {
            return;
        }

        refreshMetadata();

        if (!topicMetadata.containsKey(topic)) {
            createTopic(topic, 1, (short) 1);
            refreshMetadata();
        }
    }

    public record TopicMetadata(String topic, Map<Integer, BrokerInfo> partitions) {
    }
}
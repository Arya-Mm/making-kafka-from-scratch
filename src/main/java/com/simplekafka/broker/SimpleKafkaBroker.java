package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleKafkaBroker {

    private final int brokerId;
    private final String host;
    private final int port;
    private final int zkPort;
    private final ZookeeperClient zkClient;
    private final Map<String, PartitionManager> topics = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, PartitionMetadata>> topicMetadata = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private volatile boolean isController = false;
    private ServerSocketChannel serverChannel;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.zkPort = zkPort;
        this.zkClient = new ZookeeperClient(host, zkPort);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: java com.simplekafka.broker.SimpleKafkaBroker <brokerId> <host> <port> <zkPort>");
            System.exit(1);
        }

        int brokerId = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        int zkPort = Integer.parseInt(args[3]);

        new SimpleKafkaBroker(brokerId, host, port, zkPort).start();
    }

    public void start() throws Exception {
        zkClient.connect();
        registerBroker();
        electController();
        watchController();
        watchBrokerChanges();
        watchTopicChanges();
        loadTopicsFromZookeeper();

        serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(host, port));

        System.out.println("Broker " + brokerId + " running on " + host + ":" + port + " controller=" + isController);

        while (true) {
            SocketChannel client = serverChannel.accept();
            executor.submit(() -> handleClient(client));
        }
    }

    private void registerBroker() {

        String path = "/brokers/ids/" + brokerId;
        String data = host + ":" + port;

        boolean success = zkClient.createEphemeralNode(path, data);

        if (success) {
            System.out.println("Broker registered: " + path);
        } else {
            System.out.println("Broker already exists: " + path);
        }
    }

    private void electController() {
        boolean became = zkClient.createEphemeralNode("/controller", String.valueOf(brokerId));
        isController = became;

        if (became) {
            System.out.println("This broker is now the controller");
        } else {
            System.out.println("Controller already exists");
        }
    }

    private void watchController() {
        zkClient.watchNode("/controller", () -> {
            System.out.println("Controller changed, trying to elect again");
            electController();
        });
    }

    private void watchBrokerChanges() {
        zkClient.watchChildren("/brokers/ids", children -> {
            System.out.println("Broker registry changed: " + children);
        });
    }

    private void watchTopicChanges() {
        zkClient.watchChildren("/topics", children -> {
            for (String topic : children) {
                if (topic == null || topic.isBlank()) {
                    continue;
                }

                if (!topicMetadata.containsKey(topic)) {
                    loadTopic(topic);
                }
            }
        });
    }

    private void loadTopicsFromZookeeper() {
        for (String topic : zkClient.getChildren("/topics")) {
            if (topic == null || topic.isBlank()) {
                continue;
            }
            loadTopic(topic);
        }
    }

    private void loadTopic(String topic) {
        try {
            String partitionPath = "/topics/" + topic + "/partitions";
            List<String> partitionIds = zkClient.getChildren(partitionPath);
            if (partitionIds.isEmpty()) {
                return;
            }

            int partitionCount = partitionIds.size();
            PartitionManager manager = new PartitionManager(topic, partitionCount);
            topics.put(topic, manager);

            Map<Integer, PartitionMetadata> partitionMetadata = new ConcurrentHashMap<>();

            for (String partitionId : partitionIds) {
                int pid = Integer.parseInt(partitionId);
                String state = zkClient.getData(partitionPath + "/" + partitionId + "/state");
                partitionMetadata.put(pid, parsePartitionState(state));
                watchPartitionState(topic, pid);
            }

            topicMetadata.put(topic, partitionMetadata);
            System.out.println("Loaded topic " + topic + " with " + partitionCount + " partitions");
        } catch (Exception e) {
            System.err.println("Failed to load topic metadata for " + topic + ": " + e.getMessage());
        }
    }

    private void watchPartitionState(String topic, int partition) {
        String statePath = "/topics/" + topic + "/partitions/" + partition + "/state";
        zkClient.watchNode(statePath, () -> {
            try {
                String state = zkClient.getData(statePath);
                Map<Integer, PartitionMetadata> partitionMap = topicMetadata.get(topic);
                if (partitionMap != null) {
                    partitionMap.put(partition, parsePartitionState(state));
                    System.out.println("Reloaded partition state for " + topic + "-" + partition + ": " + state);
                }
            } catch (Exception e) {
                System.err.println("Failed to reload partition state for " + topic + "-" + partition + ": " + e.getMessage());
            }
        });
    }

    private PartitionMetadata parsePartitionState(String state) {

        if (state == null || state.isBlank()) {
            return new PartitionMetadata(brokerId, Collections.emptyList());
        }

        int leader = brokerId;
        List<Integer> followers = new ArrayList<>();

        for (String part : state.split(",")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("leader=")) {
                String value = trimmed.substring("leader=".length()).trim();
                try {
                    leader = Integer.parseInt(value);
                } catch (NumberFormatException ignored) {
                }
            } else if (trimmed.startsWith("followers=")) {
                String followerPart = trimmed.substring("followers=".length()).trim();
                if (!followerPart.isBlank()) {
                    for (String followerId : followerPart.split(";")) {
                        if (!followerId.isBlank()) {
                            try {
                                followers.add(Integer.parseInt(followerId.trim()));
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                }
            }
        }

        return new PartitionMetadata(leader, followers);
    }

    private void storePartitionState(String topic, int partition, int leader, List<Integer> followers) {
        String followerValue = String.join(";", followers.stream().map(String::valueOf).toList());
        String state = "leader=" + leader + ",followers=" + followerValue;
        zkClient.createPersistentNode("/topics/" + topic + "/partitions/" + partition + "/state", state);
    }

    private void createTopic(String topic, int numPartitions, short replicationFactor) throws Exception {
        if (topicMetadata.containsKey(topic)) {
            return;
        }

        String topicPath = "/topics/" + topic;
        zkClient.createPersistentNode(topicPath, "partitions=" + numPartitions + ",replication=" + replicationFactor);
        zkClient.createPersistentNode(topicPath + "/partitions", "");

        List<BrokerInfo> brokers = zkClient.getAllBrokerInfo();
        if (brokers.isEmpty()) {
            brokers = List.of(new BrokerInfo(brokerId, host, port));
        }

        int replicaCount = Math.min(replicationFactor, (short) brokers.size());
        Map<Integer, PartitionMetadata> metadata = new ConcurrentHashMap<>();
        PartitionManager manager = new PartitionManager(topic, numPartitions);

        for (int partition = 0; partition < numPartitions; partition++) {
            int leaderIndex = partition % brokers.size();
            BrokerInfo leader = brokers.get(leaderIndex);

            List<Integer> followers = new ArrayList<>();
            for (int i = 1; i < replicaCount; i++) {
                followers.add(brokers.get((leaderIndex + i) % brokers.size()).getId());
            }

            metadata.put(partition, new PartitionMetadata(leader.getId(), followers));
            storePartitionState(topic, partition, leader.getId(), followers);
            watchPartitionState(topic, partition);
        }

        topics.put(topic, manager);
        topicMetadata.put(topic, metadata);

        notifyTopicCreation(topic);

        System.out.println("Created topic " + topic + ", partitions=" + numPartitions + ", replication=" + replicationFactor);
    }

    private void notifyTopicCreation(String topic) {
        for (BrokerInfo broker : getBrokerList()) {
            if (broker.getId() == brokerId) {
                continue;
            }

            executor.submit(() -> {
                try (SocketChannel socket = SocketChannel.open()) {
                    socket.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));
                    socket.write(Protocol.encodeTopicNotification(topic));
                } catch (IOException e) {
                    System.err.println("Failed to notify broker " + broker.getId() + " about topic creation: " + e.getMessage());
                }
            });
        }
    }

    private Map<String, Map<Integer, Integer>> buildTopicLeaderMap() {
        Map<String, Map<Integer, Integer>> leaderMap = new HashMap<>();

        topicMetadata.forEach((topic, partitionMap) -> {
            Map<Integer, Integer> map = new HashMap<>();
            partitionMap.forEach((pid, metadata) -> map.put(pid, metadata.leader));
            leaderMap.put(topic, map);
        });

        return leaderMap;
    }

    private List<BrokerInfo> getBrokerList() {
        return zkClient.getAllBrokerInfo();
    }

    private BrokerInfo getBrokerInfo(int brokerId) {
        return getBrokerList().stream()
                .filter(info -> info.getId() == brokerId)
                .findFirst()
                .orElse(null);
    }

    private void handleClient(SocketChannel client) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(4096);

            while (client.isOpen()) {
                buffer.clear();
                int bytesRead = client.read(buffer);

                if (bytesRead == -1) {
                    client.close();
                    break;
                }

                if (bytesRead == 0) {
                    continue;
                }

                buffer.flip();
                processClientMessage(client, buffer);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (client.isOpen()) {
                    client.close();
                }
            } catch (IOException ignored) {
            }
        }
    }

    private void processClientMessage(SocketChannel client, ByteBuffer buffer) throws IOException {
        byte type = buffer.get();

        switch (type) {
            case Protocol.PRODUCE -> handleProduceRequest(client, buffer);
            case Protocol.FETCH -> handleFetchRequest(client, buffer);
            case Protocol.METADATA -> handleMetadataRequest(client);
            case Protocol.CREATE_TOPIC -> handleCreateTopicRequest(client, buffer);
            case Protocol.REPLICATE -> handleReplicateRequest(client, buffer);
            case Protocol.TOPIC_NOTIFICATION -> handleTopicNotification(client, buffer);
            default -> Protocol.sendErrorResponse(client, "Unknown message type: " + type);
        }
    }

    private void handleMetadataRequest(SocketChannel client) throws IOException {
        client.write(Protocol.encodeMetadataResponse(getBrokerList(), buildTopicLeaderMap()));
    }

    private void handleCreateTopicRequest(SocketChannel client, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);

        String topic = new String(topicBytes);
        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        if (!isController) {
            Protocol.sendErrorResponse(client, "CreateTopic request only allowed on controller");
            return;
        }

        try {
            createTopic(topic, numPartitions, replicationFactor);
            client.write(Protocol.encodeCreateTopicResponse(true, "Topic created"));
        } catch (Exception e) {
            Protocol.sendErrorResponse(client, "Failed to create topic: " + e.getMessage());
        }
    }

    private void handleTopicNotification(SocketChannel client, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);

        String topic = new String(topicBytes);
        System.out.println("Topic notification received: " + topic);
        loadTopic(topic);
    }

    private void handleProduceRequest(SocketChannel client, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);

        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);

        if (!topicMetadata.containsKey(topic)) {
            loadTopic(topic);
        }

        if (!topicMetadata.containsKey(topic)) {
            if (isController) {
                try {
                    createTopic(topic, Math.max(1, partition + 1), (short) 1);
                } catch (Exception e) {
                    Protocol.sendErrorResponse(client, "Failed to auto-create topic: " + e.getMessage());
                    return;
                }
            } else {
                Protocol.sendErrorResponse(client, "Unknown topic: " + topic);
                return;
            }
        }

        Map<Integer, PartitionMetadata> partitionMap = topicMetadata.get(topic);
        if (!partitionMap.containsKey(partition)) {
            Protocol.sendErrorResponse(client, "Unknown partition: " + partition);
            return;
        }

        PartitionMetadata metadata = partitionMap.get(partition);
        if (metadata.leader != brokerId) {
            BrokerInfo leaderBroker = getBrokerInfo(metadata.leader);
            if (leaderBroker == null) {
                Protocol.sendErrorResponse(client, "Leader not available for partition " + partition);
                return;
            }

            try (SocketChannel leaderSocket = SocketChannel.open()) {
                leaderSocket.connect(new InetSocketAddress(leaderBroker.getHost(), leaderBroker.getPort()));
                ByteBuffer forwardRequest = Protocol.encodeProduceRequest(topic, partition, message);
                leaderSocket.write(forwardRequest);

                ByteBuffer response = ByteBuffer.allocate(1024);
                int bytesRead = leaderSocket.read(response);
                if (bytesRead <= 0) {
                    Protocol.sendErrorResponse(client, "No response from leader broker");
                    return;
                }
                response.flip();
                client.write(response);
            } catch (IOException e) {
                Protocol.sendErrorResponse(client, "Failed to forward to leader: " + e.getMessage());
            }
            return;
        }

        Partition partitionLog = topics.get(topic).getPartition(partition);
        long offset = partitionLog.append(message);
        replicateToFollowers(topic, partition, message, offset);
        client.write(Protocol.encodeProduceResponse(offset));
    }

    private void handleFetchRequest(SocketChannel client, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);

        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxMessages = buffer.getInt();
        short groupLen = buffer.getShort();
        byte[] groupBytes = new byte[groupLen];
        buffer.get(groupBytes);

        if (!topics.containsKey(topic)) {
            loadTopic(topic);
        }

        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(client, "Unknown topic: " + topic);
            return;
        }

        Partition partitionLog = topics.get(topic).getPartition(partition);
        if (partitionLog == null) {
            Protocol.sendErrorResponse(client, "Unknown partition: " + partition);
            return;
        }

        List<byte[]> messages = partitionLog.readFromOffset(offset, maxMessages);
        client.write(Protocol.encodeFetchResponse(messages.toArray(new byte[0][])));
    }

    private void handleReplicateRequest(SocketChannel client, ByteBuffer buffer) throws IOException {
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);

        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);

        if (!topics.containsKey(topic)) {
            loadTopic(topic);
        }

        if (!topics.containsKey(topic)) {
            Protocol.sendErrorResponse(client, "Unknown topic: " + topic);
            return;
        }

        Partition partitionLog = topics.get(topic).getPartition(partition);
        if (partitionLog == null) {
            Protocol.sendErrorResponse(client, "Unknown partition: " + partition);
            return;
        }

        try {
            long writtenOffset = partitionLog.append(message, offset);
            client.write(Protocol.encodeProduceResponse(writtenOffset));
        } catch (IOException e) {
            Protocol.sendErrorResponse(client, "Replication failed: " + e.getMessage());
        }
    }

    private void replicateToFollowers(String topic, int partition, byte[] message, long offset) {
        Map<Integer, PartitionMetadata> partitionMetadata = topicMetadata.get(topic);
        if (partitionMetadata == null) {
            return;
        }

        PartitionMetadata metadata = partitionMetadata.get(partition);
        if (metadata == null || metadata.followers.isEmpty()) {
            return;
        }

        for (int followerId : metadata.followers) {
            BrokerInfo followerBroker = getBrokerInfo(followerId);
            if (followerBroker == null) {
                System.err.println("Follower broker metadata unavailable for id: " + followerId);
                continue;
            }

            executor.submit(() -> {
                try (SocketChannel socket = SocketChannel.open()) {
                    socket.connect(new InetSocketAddress(followerBroker.getHost(), followerBroker.getPort()));
                    socket.write(Protocol.encodeReplicateRequest(topic, partition, offset, message));

                    ByteBuffer response = ByteBuffer.allocate(1024);
                    int bytesRead = socket.read(response);
                    if (bytesRead <= 0) {
                        System.err.println("No replication ack from follower " + followerId);
                        return;
                    }
                    response.flip();
                    Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);
                    if (result.error() != null) {
                        System.err.println("Replication failed for follower " + followerId + ": " + result.error());
                    } else {
                        System.out.println("Replicated offset " + offset + " to follower " + followerId);
                    }
                } catch (IOException e) {
                    System.err.println("Replication exception to follower " + followerId + ": " + e.getMessage());
                }
            });
        }
    }
}
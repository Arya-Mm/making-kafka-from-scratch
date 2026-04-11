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
                }
            } catch (Exception e) {
                System.err.println("Failed to reload partition state for " + topic + "-" + partition + ": " + e.getMessage());
            }
        });
    }

    private PartitionMetadata parsePartitionState(String state) {
        int leader = brokerId;
        List<Integer> followers = new ArrayList<>();

        if (state == null || state.isBlank()) {
            return new PartitionMetadata(leader, followers);
        }

        for (String part : state.split(",")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("leader=")) {
                String value = trimmed.substring("leader=".length()).trim();
                try {
                    leader = Integer.parseInt(value);
                } catch (NumberFormatException ignored) {
                }
            } else if (trimmed.startsWith("followers=")) {
                String value = trimmed.substring("followers=".length()).trim();
                if (!value.isBlank()) {
                    for (String followerId : value.split(";")) {
                        try {
                            followers.add(Integer.parseInt(followerId.trim()));
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
            }
        }

        return new PartitionMetadata(leader, followers);
    }

    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            int bytesRead = clientChannel.read(buffer);
            if (bytesRead <= 0) {
                return;
            }

            buffer.flip();
            processClientMessage(clientChannel, buffer);
        } catch (Exception e) {
            System.err.println("Client handler error: " + e.getMessage());
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                System.err.println("Failed to close client channel: " + e.getMessage());
            }
        }
    }

    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        ByteBuffer requestCopy = buffer.duplicate();
        String topic = readString(buffer);
        int partition = buffer.getInt();
        int length = buffer.getInt();
        byte[] message = new byte[length];
        buffer.get(message);

        if (!topicMetadata.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Unknown topic: " + topic);
            return;
        }

        BrokerInfo leader = getLeader(topic, partition);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown partition: " + partition);
            return;
        }

        if (leader.getId() != brokerId) {
            forwardRequest(clientChannel, requestCopy, leader);
            return;
        }

        PartitionManager manager = topics.get(topic);
        if (manager == null) {
            Protocol.sendErrorResponse(clientChannel, "Topic not loaded: " + topic);
            return;
        }

        try {
            long offset = manager.getPartition(partition).append(message);
            replicateToFollowers(topic, partition, message, offset);
            clientChannel.write(Protocol.encodeProduceResponse(offset));
        } catch (IOException e) {
            Protocol.sendErrorResponse(clientChannel, "Produce failed: " + e.getMessage());
        }
    }

    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        ByteBuffer requestCopy = buffer.duplicate();
        String topic = readString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxMessages = buffer.getInt();
        int groupLength = buffer.getShort();
        if (groupLength > 0) {
            byte[] groupBytes = new byte[groupLength];
            buffer.get(groupBytes);
        }

        if (!topicMetadata.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Unknown topic: " + topic);
            return;
        }

        BrokerInfo leader = getLeader(topic, partition);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown partition: " + partition);
            return;
        }

        if (leader.getId() != brokerId) {
            forwardRequest(clientChannel, requestCopy, leader);
            return;
        }

        PartitionManager manager = topics.get(topic);
        if (manager == null) {
            Protocol.sendErrorResponse(clientChannel, "Topic not loaded: " + topic);
            return;
        }

        try {
            List<byte[]> messages = manager.getPartition(partition).readFromOffset(offset, maxMessages);
            byte[][] payload = messages.toArray(new byte[0][]);
            clientChannel.write(Protocol.encodeFetchResponse(payload));
        } catch (IOException e) {
            Protocol.sendErrorResponse(clientChannel, "Fetch failed: " + e.getMessage());
        }
    }

    private void handleMetadataRequest(SocketChannel clientChannel) throws IOException {
        List<BrokerInfo> brokers = getBrokerList();
        Map<String, Map<Integer, Integer>> topicLeaders = new HashMap<>();

        for (Map.Entry<String, Map<Integer, PartitionMetadata>> entry : topicMetadata.entrySet()) {
            Map<Integer, Integer> leaders = new HashMap<>();
            for (Map.Entry<Integer, PartitionMetadata> partitionEntry : entry.getValue().entrySet()) {
                leaders.put(partitionEntry.getKey(), partitionEntry.getValue().leader);
            }
            topicLeaders.put(entry.getKey(), leaders);
        }

        clientChannel.write(Protocol.encodeMetadataResponse(brokers, topicLeaders));
    }

    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        if (!isController) {
            BrokerInfo controller = getControllerBroker();
            if (controller == null) {
                Protocol.sendErrorResponse(clientChannel, "No controller available");
                return;
            }
            forwardRequest(clientChannel, buffer, controller); // message already has type consumed, but this is rare path
            return;
        }

        try {
            createTopic(topic, numPartitions, replicationFactor);
            clientChannel.write(Protocol.encodeCreateTopicResponse(true, "Topic created"));
        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, "Create topic failed: " + e.getMessage());
        }
    }

    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int length = buffer.getInt();
        byte[] message = new byte[length];
        buffer.get(message);

        PartitionManager manager = topics.get(topic);
        if (manager == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown topic: " + topic);
            return;
        }

        try {
            manager.getPartition(partition).append(message, offset);
            clientChannel.write(Protocol.encodeProduceResponse(offset));
        } catch (IOException e) {
            Protocol.sendErrorResponse(clientChannel, "Replication failed: " + e.getMessage());
        }
    }

    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        String topic = readString(buffer);
        if (!topicMetadata.containsKey(topic)) {
            loadTopic(topic);
        }
        clientChannel.write(Protocol.encodeCreateTopicResponse(true, "Topic notification processed"));
    }

    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (topicMetadata.containsKey(topic)) {
            throw new IllegalStateException("Topic already exists: " + topic);
        }

        List<BrokerInfo> brokers = getBrokerList();
        if (brokers.isEmpty()) {
            throw new IllegalStateException("No brokers available to assign partitions");
        }

        String topicRoot = "/topics/" + topic;
        zkClient.createPersistentNode(topicRoot, "partitions=" + numPartitions);
        zkClient.createPersistentNode(topicRoot + "/partitions", "");

        Map<Integer, PartitionMetadata> partitionMetadata = new ConcurrentHashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            BrokerInfo leader = brokers.get(i % brokers.size());
            List<Integer> followers = new ArrayList<>();

            int replicas = Math.min(replicationFactor, brokers.size());
            for (int j = 0; j < replicas; j++) {
                BrokerInfo replica = brokers.get((i + j) % brokers.size());
                if (replica.getId() != leader.getId()) {
                    followers.add(replica.getId());
                }
            }

            String state = "leader=" + leader.getId() + ",followers=" + String.join(";", toStringIds(followers));
            zkClient.createPersistentNode(topicRoot + "/partitions/" + i + "/state", state);
            partitionMetadata.put(i, new PartitionMetadata(leader.getId(), followers));
        }

        topicMetadata.put(topic, partitionMetadata);
        topics.put(topic, new PartitionManager(topic, numPartitions));
        notifyTopicCreation(topic);
    }

    private List<String> toStringIds(List<Integer> ids) {
        List<String> strings = new ArrayList<>();
        for (Integer id : ids) {
            strings.add(String.valueOf(id));
        }
        return strings;
    }

    private BrokerInfo getLeader(String topic, int partition) {
        Map<Integer, PartitionMetadata> partitionMap = topicMetadata.get(topic);
        if (partitionMap == null) {
            return null;
        }
        PartitionMetadata metadata = partitionMap.get(partition);
        if (metadata == null) {
            return null;
        }
        return getBrokerInfo(metadata.leader);
    }

    private BrokerInfo getControllerBroker() {
        String controllerData = zkClient.getData("/controller");
        if (controllerData == null || controllerData.isBlank()) {
            return null;
        }
        try {
            int controllerId = Integer.parseInt(controllerData.trim());
            return getBrokerInfo(controllerId);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private void forwardRequest(SocketChannel clientChannel, ByteBuffer requestCopy, BrokerInfo targetBroker) throws IOException {
        byte[] requestBytes = new byte[requestCopy.remaining()];
        requestCopy.get(requestBytes);
        forwardRequest(clientChannel, requestBytes, targetBroker);
    }

    private void forwardRequest(SocketChannel clientChannel, byte[] requestBytes, BrokerInfo targetBroker) throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.connect(new InetSocketAddress(targetBroker.getHost(), targetBroker.getPort()));
            ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
            while (requestBuffer.hasRemaining()) {
                socket.write(requestBuffer);
            }

            ByteBuffer response = ByteBuffer.allocate(8192);
            int bytesRead = socket.read(response);
            if (bytesRead <= 0) {
                Protocol.sendErrorResponse(clientChannel, "No response from leader");
                return;
            }
            response.flip();
            while (response.hasRemaining()) {
                clientChannel.write(response);
            }
        }
    }

    private BrokerInfo getBrokerInfo(int brokerId) {
        for (BrokerInfo broker : getBrokerList()) {
            if (broker.getId() == brokerId) {
                return broker;
            }
        }
        return null;
    }

    private List<BrokerInfo> getBrokerList() {
        return zkClient.getAllBrokerInfo();
    }

    private String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
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
                    System.err.println("Failed to notify broker " + broker.getId() + " about topic " + topic + ": " + e.getMessage());
                }
            });
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
                System.err.println("Follower broker not available: " + followerId);
                continue;
            }

            executor.submit(() -> {
                try (SocketChannel socket = SocketChannel.open()) {
                    socket.connect(new InetSocketAddress(followerBroker.getHost(), followerBroker.getPort()));
                    socket.write(Protocol.encodeReplicateRequest(topic, partition, offset, message));

                    ByteBuffer response = ByteBuffer.allocate(1024);
                    int bytesRead = socket.read(response);
                    if (bytesRead > 0) {
                        response.flip();
                        Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);
                        if (result.error() != null) {
                            System.err.println("Replication error to follower " + followerId + ": " + result.error());
                        }
                    } else {
                        System.err.println("No replication ack from follower " + followerId);
                    }
                } catch (IOException e) {
                    System.err.println("Failed to replicate to follower " + followerId + ": " + e.getMessage());
                }
            });
        }
    }
}
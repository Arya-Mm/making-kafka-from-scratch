package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SimpleKafkaBroker {

    private static PartitionManager partitionManager =
            new PartitionManager("test", 3);

    private static ConsumerGroupManager groupManager =
            new ConsumerGroupManager();

    public static void main(String[] args) throws IOException {

        int port = Integer.parseInt(args[0]);
        int brokerId = Integer.parseInt(args[1]);

        // Register broker + assign leaders
        ClusterManager.registerBroker(brokerId);
        ClusterManager.assignLeaders(3);

        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(port));

        System.out.println("Broker " + brokerId + " running on port " + port);

        while (true) {
            SocketChannel client = server.accept();
            new Thread(() -> handleClient(client, brokerId)).start();
        }
    }

    private static void handleClient(SocketChannel client, int brokerId) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            while (client.isOpen()) {

                buffer.clear();
                int bytesRead = client.read(buffer);

                if (bytesRead == -1) {
                    client.close();
                    break;
                }

                buffer.flip();
                byte messageType = buffer.get();

                // ================= PRODUCE =================
                if (messageType == Protocol.PRODUCE) {

                    short topicLength = buffer.getShort();
                    byte[] topicBytes = new byte[topicLength];
                    buffer.get(topicBytes);
                    String topic = new String(topicBytes);

                    int partitionId = buffer.getInt();

                    int leader = ClusterManager.getLeader(partitionId);

                    if (leader != brokerId) {
                        client.write(Protocol.encodeError("Not leader"));
                        continue;
                    }

                    int msgLen = buffer.getInt();
                    byte[] message = new byte[msgLen];
                    buffer.get(message);

                    Partition partition = partitionManager.getPartition(partitionId);
                    long offset = partition.append(message);

                    System.out.println("Leader " + brokerId +
                            " stored message at offset " + offset);

                    // Followers = all except leader
                    List<Integer> followers =
                            ClusterManager.getActiveBrokers()
                                    .stream()
                                    .filter(id -> id != brokerId)
                                    .toList();

                    // Replication
                    for (int follower : followers) {

                        new Thread(() -> {
                            try {
                                SocketChannel followerSocket = SocketChannel.open();
                                followerSocket.connect(
                                        new InetSocketAddress("localhost", 9092 + follower)
                                );

                                ByteBuffer replicateRequest =
                                        Protocol.encodeReplicateRequest(
                                                topic,
                                                partitionId,
                                                offset,
                                                message
                                        );

                                followerSocket.write(replicateRequest);
                                followerSocket.close();

                                System.out.println("Replicated to broker " + follower);

                            } catch (Exception e) {
                                System.out.println("Replication failed to broker " + follower);
                            }
                        }).start();
                    }

                    client.write(Protocol.encodeProduceResponse(offset));
                }

                // ================= REPLICATE =================
                else if (messageType == Protocol.REPLICATE) {

                    short topicLength = buffer.getShort();
                    byte[] topicBytes = new byte[topicLength];
                    buffer.get(topicBytes);

                    int partitionId = buffer.getInt();
                    long offset = buffer.getLong();

                    int msgLen = buffer.getInt();
                    byte[] message = new byte[msgLen];
                    buffer.get(message);

                    Partition partition = partitionManager.getPartition(partitionId);
                    partition.append(message);

                    System.out.println("Follower " + brokerId +
                            " replicated message at offset " + offset);

                    client.write(Protocol.encodeProduceResponse(offset));
                }

                // ================= FETCH =================
                else if (messageType == Protocol.FETCH) {

                    short topicLength = buffer.getShort();
                    byte[] topicBytes = new byte[topicLength];
                    buffer.get(topicBytes);

                    int partitionId = buffer.getInt();

                    buffer.getLong(); // ignore client offset
                    int maxMessages = buffer.getInt();

                    short groupLen = buffer.getShort();
                    byte[] groupBytes = new byte[groupLen];
                    buffer.get(groupBytes);
                    String groupId = new String(groupBytes);

                    Partition partition = partitionManager.getPartition(partitionId);

                    long groupOffset = groupManager.getOffset(groupId, partitionId);

                    List<byte[]> messages =
                            partition.readFromOffset(groupOffset, maxMessages);

                    groupManager.commitOffset(
                            groupId,
                            partitionId,
                            groupOffset + messages.size()
                    );

                    byte[][] responseMessages =
                            messages.toArray(new byte[0][]);

                    client.write(Protocol.encodeFetchResponse(responseMessages));
                }

                else {
                    client.write(Protocol.encodeError("Unknown request"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
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
            ByteBuffer buffer = ByteBuffer.allocate(4096);

            while (client.isOpen()) {

                buffer.clear();
                int bytesRead = client.read(buffer);

                if (bytesRead == -1) {
                    client.close();
                    break;
                }

                buffer.flip();
                byte type = buffer.get();

                // ================= PRODUCE =================
                if (type == Protocol.PRODUCE) {

                    short topicLen = buffer.getShort();
                    byte[] topicBytes = new byte[topicLen];
                    buffer.get(topicBytes);

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

                    System.out.println("Leader " + brokerId + " stored offset " + offset);

                    List<Integer> followers =
                            ClusterManager.getActiveBrokers()
                                    .stream()
                                    .filter(id -> id != brokerId)
                                    .toList();

                    for (int follower : followers) {

                        new Thread(() -> {
                            try {
                                SocketChannel s = SocketChannel.open();
                                s.connect(new InetSocketAddress("localhost", 9092 + follower));

                                s.write(Protocol.encodeReplicateRequest(
                                        "test",
                                        partitionId,
                                        offset,
                                        message
                                ));

                                s.close();

                            } catch (Exception ignored) {}
                        }).start();
                    }

                    client.write(Protocol.encodeProduceResponse(offset));
                }

                // ================= REPLICATE =================
                else if (type == Protocol.REPLICATE) {

                    short topicLen = buffer.getShort();
                    buffer.position(buffer.position() + topicLen);

                    int partitionId = buffer.getInt();
                    long offset = buffer.getLong();

                    int msgLen = buffer.getInt();
                    byte[] message = new byte[msgLen];
                    buffer.get(message);

                    Partition partition = partitionManager.getPartition(partitionId);
                    partition.append(message);

                    System.out.println("Follower " + brokerId + " replicated offset " + offset);

                    client.write(Protocol.encodeProduceResponse(offset));
                }

                // ================= FETCH =================
                else if (type == Protocol.FETCH) {

                    short topicLen = buffer.getShort();
                    byte[] topicBytes = new byte[topicLen];
                    buffer.get(topicBytes);

                    int partitionId = buffer.getInt();

                    buffer.getLong();
                    int maxMessages = buffer.getInt();

                    short groupLen = buffer.getShort();
                    byte[] groupBytes = new byte[groupLen];
                    buffer.get(groupBytes);
                    String groupId = new String(groupBytes);

                    Partition partition = partitionManager.getPartition(partitionId);

                    long offset = groupManager.getOffset(groupId, partitionId);

                    List<byte[]> messages =
                            partition.readFromOffset(offset, maxMessages);

                    groupManager.commitOffset(
                            groupId,
                            partitionId,
                            offset + messages.size()
                    );

                    client.write(Protocol.encodeFetchResponse(
                            messages.toArray(new byte[0][])
                    ));
                }

                // ================= METADATA =================
                else if (type == Protocol.METADATA) {

                    client.write(Protocol.encodeMetadataResponse());
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
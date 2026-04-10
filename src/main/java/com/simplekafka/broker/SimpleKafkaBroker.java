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

                // 🔥 PRODUCE
                if (messageType == Protocol.PRODUCE) {

                    short topicLength = buffer.getShort();
                    byte[] topicBytes = new byte[topicLength];
                    buffer.get(topicBytes);

                    int partitionId = buffer.getInt();

                    // 🔥 CHECK OWNERSHIP
                    if (getBrokerForPartition(partitionId) != brokerId) {
                        client.write(Protocol.encodeError("Wrong broker"));
                        continue;
                    }

                    int messageLength = buffer.getInt();
                    byte[] message = new byte[messageLength];
                    buffer.get(message);

                    Partition partition = partitionManager.getPartition(partitionId);
                    long offset = partition.append(message);

                    System.out.println("Broker " + brokerId +
                            " stored message in partition " + partitionId);

                    client.write(Protocol.encodeProduceResponse(offset));
                }

                // 🔥 FETCH
                else if (messageType == Protocol.FETCH) {

                    short topicLength = buffer.getShort();
                    byte[] topicBytes = new byte[topicLength];
                    buffer.get(topicBytes);

                    int partitionId = buffer.getInt();

                    if (getBrokerForPartition(partitionId) != brokerId) {
                        client.write(Protocol.encodeError("Wrong broker"));
                        continue;
                    }

                    buffer.getLong(); // ignore offset
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

    // 🔥 PARTITION → BROKER MAPPING
    private static int getBrokerForPartition(int partitionId) {
        return partitionId % 3;
    }
}
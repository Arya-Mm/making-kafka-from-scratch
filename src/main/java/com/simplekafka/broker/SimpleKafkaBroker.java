package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SimpleKafkaBroker {

    private static final int PORT = 9092;

    private static final PartitionManager partitionManager =
            new PartitionManager("test", 3);

    private static final ConsumerGroupManager groupManager =
            new ConsumerGroupManager();

    public static void main(String[] args) throws IOException {
        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(PORT));

        System.out.println("Broker running on port " + PORT);

        while (true) {
            SocketChannel client = server.accept();
            System.out.println("Client connected");

            new Thread(() -> handleClient(client)).start();
        }
    }

    private static void handleClient(SocketChannel client) {
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

                // PRODUCE
                if (messageType == Protocol.PRODUCE) {
                    try {
                        short topicLength = buffer.getShort();
                        byte[] topicBytes = new byte[topicLength];
                        buffer.get(topicBytes);

                        int partitionId = buffer.getInt();

                        int messageLength = buffer.getInt();
                        byte[] message = new byte[messageLength];
                        buffer.get(message);

                        Partition partition = partitionManager.getPartition(partitionId);
                        long offset = partition.append(message);

                        System.out.println("Partition: " + partitionId);
                        System.out.println("Stored: " + new String(message));
                        System.out.println("Offset: " + offset);

                        ByteBuffer response = Protocol.encodeProduceResponse(offset);
                        client.write(response);

                    } catch (Exception e) {
                        e.printStackTrace();
                        client.write(Protocol.encodeError("Produce failed"));
                    }
                }

                // FETCH WITH GROUP
                else if (messageType == Protocol.FETCH) {
                    try {
                        short topicLength = buffer.getShort();
                        byte[] topicBytes = new byte[topicLength];
                        buffer.get(topicBytes);

                        int partitionId = buffer.getInt();
                        long ignoredOffset = buffer.getLong(); // ignored
                        int maxMessages = buffer.getInt();

                        short groupLen = buffer.getShort();
                        byte[] groupBytes = new byte[groupLen];
                        buffer.get(groupBytes);
                        String groupId = new String(groupBytes);

                        Partition partition = partitionManager.getPartition(partitionId);

                        long groupOffset = groupManager.getOffset(groupId, partitionId);

                        List<byte[]> messages =
                                partition.readFromOffset(groupOffset, maxMessages);

                        byte[][] responseMessages =
                                messages.toArray(new byte[0][]);

                        groupManager.commitOffset(
                                groupId,
                                partitionId,
                                groupOffset + messages.size()
                        );

                        ByteBuffer response =
                                Protocol.encodeFetchResponse(responseMessages);

                        client.write(response);

                    } catch (Exception e) {
                        e.printStackTrace();
                        client.write(Protocol.encodeError("Fetch failed"));
                    }
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
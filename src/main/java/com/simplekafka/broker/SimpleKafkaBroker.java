package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

public class SimpleKafkaBroker {

    private static final int PORT = 9092;
    private static final Partition partition = new Partition("test");

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

                // 🔥 PRODUCE
                if (messageType == Protocol.PRODUCE) {
                    System.out.println("Received PRODUCE request");

                    try {
                        short topicLength = buffer.getShort();
                        byte[] topicBytes = new byte[topicLength];
                        buffer.get(topicBytes);

                        int partitionId = buffer.getInt();

                        int messageLength = buffer.getInt();
                        byte[] message = new byte[messageLength];
                        buffer.get(message);

                        long offset = partition.append(message);

                        System.out.println("Stored message: " + new String(message));
                        System.out.println("Offset: " + offset);

                        ByteBuffer response = Protocol.encodeProduceResponse(offset);
                        client.write(response);

                    } catch (Exception e) {
                        e.printStackTrace();
                        ByteBuffer error = Protocol.encodeError("Produce failed");
                        client.write(error);
                    }
                }

                // 🔥 FETCH (REAL)
                else if (messageType == Protocol.FETCH) {
                    System.out.println("Received FETCH request");

                    try {
                        short topicLength = buffer.getShort();
                        byte[] topicBytes = new byte[topicLength];
                        buffer.get(topicBytes);

                        int partitionId = buffer.getInt();
                        long offset = buffer.getLong();
                        int maxMessages = buffer.getInt();

                        List<byte[]> messages = partition.readFromOffset(offset, maxMessages);

                        byte[][] responseMessages = messages.toArray(new byte[0][]);

                        ByteBuffer response = Protocol.encodeFetchResponse(responseMessages);
                        client.write(response);

                    } catch (Exception e) {
                        e.printStackTrace();
                        ByteBuffer error = Protocol.encodeError("Fetch failed");
                        client.write(error);
                    }
                }

                // 🔥 ERROR
                else {
                    System.out.println("Unknown request");
                    ByteBuffer error = Protocol.encodeError("Unknown request");
                    client.write(error);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class SimpleKafkaBroker {

    private static final int PORT = 9092;

    // 🔥 Storage engine
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

                // 🔥 PRODUCE (REAL PARSING + STORAGE)
                if (messageType == Protocol.PRODUCE) {
                    System.out.println("Received PRODUCE request");

                    try {
                        // Read topic
                        short topicLength = buffer.getShort();
                        byte[] topicBytes = new byte[topicLength];
                        buffer.get(topicBytes);
                        String topic = new String(topicBytes);

                        // Read partition
                        int partitionId = buffer.getInt();

                        // Read message
                        int messageLength = buffer.getInt();
                        byte[] message = new byte[messageLength];
                        buffer.get(message);

                        // Store message
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

                // 🔥 FETCH (still basic for now)
                else if (messageType == Protocol.FETCH) {
                    System.out.println("Received FETCH request");

                    byte[][] messages = new byte[][] {
                            "hello".getBytes(),
                            "world".getBytes()
                    };

                    ByteBuffer response = Protocol.encodeFetchResponse(messages);
                    client.write(response);
                }

                // 🔥 UNKNOWN REQUEST
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
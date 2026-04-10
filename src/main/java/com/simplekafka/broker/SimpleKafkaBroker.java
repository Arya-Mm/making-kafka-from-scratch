package com.simplekafka.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class SimpleKafkaBroker {

    private static ZookeeperClient zkClient;

    public static void main(String[] args) throws Exception {

        int brokerId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);

        // 🔥 CONNECT TO ZOOKEEPER
        zkClient = new ZookeeperClient("localhost", 2181);
        zkClient.connect();

        // 🔥 REGISTER BROKER
        registerBroker(brokerId, "localhost", port);

        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(port));

        System.out.println("Broker " + brokerId + " running on port " + port);

        while (true) {
            SocketChannel client = server.accept();
            new Thread(() -> handleClient(client)).start();
        }
    }

    // ================= REGISTER =================
    private static void registerBroker(int id, String host, int port) {

        String path = "/brokers/ids/" + id;

        String data = host + ":" + port;

        boolean success = zkClient.createEphemeralNode(path, data);

        if (success) {
            System.out.println("Broker registered: " + path);
        } else {
            System.out.println("Broker already exists: " + path);
        }
    }

    // ================= CLIENT HANDLER =================
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

                byte type = buffer.get();

                // 🔥 JUST KEEP BASIC FOR NOW
                if (type == Protocol.METADATA) {
                    client.write(Protocol.encodeMetadataResponse());
                } else {
                    client.write(Protocol.encodeError("Not implemented yet"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
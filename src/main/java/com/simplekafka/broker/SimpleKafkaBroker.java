package com.simplekafka.broker;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class SimpleKafkaBroker {

    private static final int PORT = 9092;

    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Broker started on port " + PORT);

        while (true) {
            Socket client = serverSocket.accept();
            System.out.println("Client connected: " + client.getInetAddress());

            new Thread(() -> handleClient(client)).start();
        }
    }

    private static void handleClient(Socket client) {
        try (
            InputStream in = client.getInputStream();
            OutputStream out = client.getOutputStream()
        ) {
            byte[] buffer = new byte[1024];

            while (true) {
                int read = in.read(buffer);
                if (read == -1) break;

                ByteBuffer request = ByteBuffer.wrap(buffer, 0, read);

                byte type = request.get();

                if (type == Protocol.PRODUCE) {
                    System.out.println("Received PRODUCE request");

                    // fake offset
                    ByteBuffer response = Protocol.encodeProduceResponse(1L);
                    out.write(response.array());

                } else if (type == Protocol.FETCH) {
                    System.out.println("Received FETCH request");

                    byte[][] messages = {
                        "hello".getBytes(),
                        "world".getBytes()
                    };

                    ByteBuffer response = Protocol.encodeFetchResponse(messages);
                    out.write(response.array());

                } else {
                    System.out.println("Unknown request");

                    ByteBuffer response = Protocol.encodeErrorResponse("Unknown request");
                    out.write(response.array());
                }
            }

        } catch (Exception e) {
            System.out.println("Client disconnected");
        }
    }
}
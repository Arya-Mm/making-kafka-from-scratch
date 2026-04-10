package com.simplekafka.client;

import com.simplekafka.broker.Protocol;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TestClient {

    public static void main(String[] args) throws Exception {

        SocketChannel socket = SocketChannel.open();
        socket.connect(new InetSocketAddress("localhost", 9092));

        System.out.println("Connected to broker");

        // 🔥 PRODUCE
        ByteBuffer request = Protocol.encodeProduceRequest(
                "test",
                0,
                "arya builds systems".getBytes()
        );

        socket.write(request);

        ByteBuffer response = ByteBuffer.allocate(1024);
        socket.read(response);
        response.flip();

        byte type = response.get();

        if (type == Protocol.PRODUCE_RESPONSE) {
            long offset = response.getLong();
            System.out.println("Message stored at offset: " + offset);
        }

        // 🔥 FETCH
        ByteBuffer fetchRequest = Protocol.encodeFetchRequest(
                "test",
                0,
                0,
                10
        );

        socket.write(fetchRequest);

        ByteBuffer fetchResponse = ByteBuffer.allocate(1024);
        socket.read(fetchResponse);
        fetchResponse.flip();

        byte fetchType = fetchResponse.get();

        if (fetchType == Protocol.FETCH_RESPONSE) {
            int messageCount = fetchResponse.getInt();

            System.out.println("Fetched messages:");

            for (int i = 0; i < messageCount; i++) {
                int len = fetchResponse.getInt();
                byte[] msg = new byte[len];
                fetchResponse.get(msg);

                System.out.println(new String(msg));
            }
        }

        socket.close();
    }
}
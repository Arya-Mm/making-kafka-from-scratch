package com.simplekafka.client;

import com.simplekafka.broker.Protocol;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TestClient {

    public static void main(String[] args) throws Exception {

        SocketChannel socket = SocketChannel.open();
        socket.connect(new InetSocketAddress("localhost", 9092));

        int partition = Math.abs("arya".hashCode()) % 3;

        // PRODUCE
        ByteBuffer request = Protocol.encodeProduceRequest(
                "test",
                partition,
                "consumer groups working".getBytes()
        );

        socket.write(request);

        ByteBuffer response = ByteBuffer.allocate(1024);
        socket.read(response);
        response.flip();

        if (response.get() == Protocol.PRODUCE_RESPONSE) {
            System.out.println("Produced message");
        }

        // FETCH using GROUP
        ByteBuffer fetchRequest = Protocol.encodeFetchRequest(
                "test",
                partition,
                0,
                10,
                "group-1"
        );

        socket.write(fetchRequest);

        ByteBuffer fetchResponse = ByteBuffer.allocate(1024);
        socket.read(fetchResponse);
        fetchResponse.flip();

        if (fetchResponse.get() == Protocol.FETCH_RESPONSE) {
            int count = fetchResponse.getInt();

            System.out.println("Fetched messages:");

            for (int i = 0; i < count; i++) {
                int len = fetchResponse.getInt();
                byte[] msg = new byte[len];
                fetchResponse.get(msg);

                System.out.println(new String(msg));
            }
        }

        socket.close();
    }
}
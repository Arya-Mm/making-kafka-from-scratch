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

        // SEND PRODUCE REQUEST
        ByteBuffer request = Protocol.encodeProduceRequest(
                "test",
                0,
                "hello kafka".getBytes()
        );

        socket.write(request);

        // READ RESPONSE
        ByteBuffer response = ByteBuffer.allocate(1024);
        socket.read(response);
        response.flip();

        byte type = response.get();

        if (type == Protocol.PRODUCE_RESPONSE) {
            long offset = response.getLong();
            System.out.println("Message stored at offset: " + offset);
        } else {
            System.out.println("Unknown response");
        }

        socket.close();
    }
}